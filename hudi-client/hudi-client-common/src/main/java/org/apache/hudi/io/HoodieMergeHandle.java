/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCorruptedDataException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("Duplicates")
/**
 * Handle to merge incoming records to those in storage.
 * <p>
 * Simplified Logic:
 * For every existing record
 *     Check if there is a new record coming in. If yes, merge two records and write to file
 *     else write the record as is
 * For all pending records from incoming batch, write to file.
 *
 * Illustration with simple data.
 * Incoming data:
 *     rec1_2, rec4_2, rec5_1, rec6_1
 * Existing data:
 *     rec1_1, rec2_1, rec3_1, rec4_1
 *
 * For every existing record, merge w/ incoming if requried and write to storage.
 *    => rec1_1 and rec1_2 is merged to write rec1_2 to storage
 *    => rec2_1 is written as is
 *    => rec3_1 is written as is
 *    => rec4_2 and rec4_1 is merged to write rec4_2 to storage
 * Write all pending records from incoming set to storage
 *    => rec5_1 and rec6_1
 *
 * Final snapshot in storage
 * rec1_2, rec2_1, rec3_1, rec4_2, rec5_1, rec6_1
 *
 * </p>
 */
public class HoodieMergeHandle<T extends HoodieRecordPayload, I, K, O> extends HoodieWriteHandle<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(HoodieMergeHandle.class);

  protected Map<String, HoodieRecord<T>> keyToNewRecords;
  protected Set<String> writtenRecordKeys;
  protected HoodieFileWriter<IndexedRecord> fileWriter;

  protected Path newFilePath;
  protected Path oldFilePath;
  protected long recordsWritten = 0;
  protected long recordsDeleted = 0;
  protected long updatedRecordsWritten = 0;
  protected long insertRecordsWritten = 0;
  protected boolean useWriterSchema;
  private HoodieBaseFile baseFileToMerge;

  public HoodieMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                           Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                           TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier);
    init(fileId, recordItr);
    init(fileId, partitionPath, hoodieTable.getBaseFileOnlyView().getLatestBaseFile(partitionPath, fileId).get());
  }

  /**
   * Called by compactor code path.
   */
  public HoodieMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                           Map<String, HoodieRecord<T>> keyToNewRecords, String partitionPath, String fileId,
                           HoodieBaseFile dataFileToBeMerged, TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier);
    this.keyToNewRecords = keyToNewRecords;
    this.useWriterSchema = true;
    init(fileId, this.partitionPath, dataFileToBeMerged);
  }

  @Override
  public Schema getWriterSchemaWithMetafields() {
    return tableSchemaWithMetaFields;
  }

  public Schema getWriterSchema() {
    return tableSchema;
  }

  /**
   * Returns the data file name.
   */
  protected String generatesDataFileName() {
    return FSUtils.makeDataFileName(instantTime, writeToken, fileId, hoodieTable.getBaseFileExtension());
  }

  /**
   * Extract old file path, initialize StorageWriter and WriteStatus.
   */
  private void init(String fileId, String partitionPath, HoodieBaseFile baseFileToMerge) {
    LOG.info("partitionPath:" + partitionPath + ", fileId to be merged:" + fileId);
    this.baseFileToMerge = baseFileToMerge;
    this.writtenRecordKeys = new HashSet<>();
    writeStatus.setStat(new HoodieWriteStat());
    try {
      String latestValidFilePath = baseFileToMerge.getFileName();
      writeStatus.getStat().setPrevCommit(FSUtils.getCommitTime(latestValidFilePath));

      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, instantTime,
          new Path(config.getBasePath()), FSUtils.getPartitionPath(config.getBasePath(), partitionPath));
      partitionMetadata.trySave(getPartitionId());

      oldFilePath = new Path(config.getBasePath() + "/" + partitionPath + "/" + latestValidFilePath);
      String newFileName = generatesDataFileName();
      String relativePath = new Path((partitionPath.isEmpty() ? "" : partitionPath + "/")
          + newFileName).toString();
      newFilePath = new Path(config.getBasePath(), relativePath);

      LOG.info(String.format("Merging new data into oldPath %s, as newPath %s", oldFilePath.toString(),
          newFilePath.toString()));
      // file name is same for all records, in this bunch
      writeStatus.setFileId(fileId);
      writeStatus.setPartitionPath(partitionPath);
      writeStatus.getStat().setPartitionPath(partitionPath);
      writeStatus.getStat().setFileId(fileId);
      writeStatus.getStat().setPath(new Path(config.getBasePath()), newFilePath);

      // Create Marker file
      createMarkerFile(partitionPath, newFileName);

      // Create the writer for writing the new version file
      fileWriter = createNewFileWriter(instantTime, newFilePath, hoodieTable, config, tableSchemaWithMetaFields, taskContextSupplier);
    } catch (IOException io) {
      LOG.error("Error in update task at commit " + instantTime, io);
      writeStatus.setGlobalError(io);
      throw new HoodieUpsertException("Failed to initialize HoodieUpdateHandle for FileId: " + fileId + " on commit "
          + instantTime + " on path " + hoodieTable.getMetaClient().getBasePath(), io);
    }
  }

  /**
   * Initialize a spillable map for incoming records.
   */
  protected void initializeIncomingRecordsMap() {
    try {
      // Load the new records in a map
      long memoryForMerge = IOUtils.getMaxMemoryPerPartitionMerge(taskContextSupplier, config.getProps());
      LOG.info("MaxMemoryPerPartitionMerge => " + memoryForMerge);
      this.keyToNewRecords = new ExternalSpillableMap<>(memoryForMerge, config.getSpillableMapBasePath(),
          new DefaultSizeEstimator(), new HoodieRecordSizeEstimator(inputSchema));
    } catch (IOException io) {
      throw new HoodieIOException("Cannot instantiate an ExternalSpillableMap", io);
    }
  }

  /**
   * Whether there is need to update the record location.
   */
  boolean needsUpdateLocation() {
    return true;
  }

  /**
   * Load the new incoming records in a map and return partitionPath.
   */
  protected void init(String fileId, Iterator<HoodieRecord<T>> newRecordsItr) {
    initializeIncomingRecordsMap();
    while (newRecordsItr.hasNext()) {
      HoodieRecord<T> record = newRecordsItr.next();
      // update the new location of the record, so we know where to find it next
      if (needsUpdateLocation()) {
        record.unseal();
        record.setNewLocation(new HoodieRecordLocation(instantTime, fileId));
        record.seal();
      }
      // NOTE: Once Records are added to map (spillable-map), DO NOT change it as they won't persist
      keyToNewRecords.put(record.getRecordKey(), record);
    }
    LOG.info("Number of entries in MemoryBasedMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getInMemoryMapNumEntries()
        + "Total size in bytes of MemoryBasedMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getCurrentInMemoryMapSize() + "Number of entries in DiskBasedMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getDiskBasedMapNumEntries() + "Size of file spilled to disk => "
        + ((ExternalSpillableMap) keyToNewRecords).getSizeOfFileOnDiskInBytes());
  }

  private boolean writeUpdateRecord(HoodieRecord<T> hoodieRecord, Option<IndexedRecord> indexedRecord) {
    if (indexedRecord.isPresent()) {
      updatedRecordsWritten++;
    }
    return writeRecord(hoodieRecord, indexedRecord);
  }

  protected boolean writeRecord(HoodieRecord<T> hoodieRecord, Option<IndexedRecord> indexedRecord) {
    Option recordMetadata = hoodieRecord.getData().getMetadata();
    if (!partitionPath.equals(hoodieRecord.getPartitionPath())) {
      HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
          + hoodieRecord.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
      writeStatus.markFailure(hoodieRecord, failureEx, recordMetadata);
      return false;
    }
    try {
      if (indexedRecord.isPresent()) {
        // Convert GenericRecord to GenericRecord with hoodie commit metadata in schema
        IndexedRecord recordWithMetadataInSchema = rewriteRecord((GenericRecord) indexedRecord.get());
        fileWriter.writeAvroWithMetadata(recordWithMetadataInSchema, hoodieRecord);
        recordsWritten++;
      } else {
        recordsDeleted++;
      }
      writeStatus.markSuccess(hoodieRecord, recordMetadata);
      // deflate record payload after recording success. This will help users access payload as a
      // part of marking
      // record successful.
      hoodieRecord.deflate();
      return true;
    } catch (Exception e) {
      LOG.error("Error writing record  " + hoodieRecord, e);
      writeStatus.markFailure(hoodieRecord, e, recordMetadata);
    }
    return false;
  }

  /**
   * Go through an old record. Here if we detect a newer version shows up, we write the new one to the file.
   */
  public void write(GenericRecord oldRecord) {
    String key = oldRecord.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
    boolean copyOldRecord = true;
    if (keyToNewRecords.containsKey(key)) {
      // If we have duplicate records that we are updating, then the hoodie record will be deflated after
      // writing the first record. So make a copy of the record to be merged
      HoodieRecord<T> hoodieRecord = new HoodieRecord<>(keyToNewRecords.get(key));
      try {
        Option<IndexedRecord> combinedAvroRecord =
            hoodieRecord.getData().combineAndGetUpdateValue(oldRecord,
              useWriterSchema ? inputSchemaWithMetaFields : inputSchema,
                config.getPayloadConfig().getProps());

        // just skip the IGNORE_RECORD
        if (combinedAvroRecord.isPresent() && combinedAvroRecord.get() == IGNORE_RECORD) {
          return;
        }
        if (writeUpdateRecord(hoodieRecord, combinedAvroRecord)) {
          /*
           * ONLY WHEN 1) we have an update for this key AND 2) We are able to successfully write the the combined new
           * value
           *
           * We no longer need to copy the old record over.
           */
          copyOldRecord = false;
        }
        writtenRecordKeys.add(key);
      } catch (Exception e) {
        throw new HoodieUpsertException("Failed to combine/merge new record with old value in storage, for new record {"
            + keyToNewRecords.get(key) + "}, old value {" + oldRecord + "}", e);
      }
    }

    if (copyOldRecord) {
      // this should work as it is, since this is an existing record
      try {
        fileWriter.writeAvro(key, oldRecord);
      } catch (IOException | RuntimeException e) {
        String errMsg = String.format("Failed to merge old record into new file for key %s from old file %s to new file %s with writerSchema %s",
                key, getOldFilePath(), newFilePath, tableSchemaWithMetaFields.toString(true));
        LOG.debug("Old record is " + oldRecord);
        throw new HoodieUpsertException(errMsg, e);
      }
      recordsWritten++;
    }
  }

  @Override
  public List<WriteStatus> close() {
    try {
      // write out any pending records (this can happen when inserts are turned into updates)
      Iterator<HoodieRecord<T>> newRecordsItr = (keyToNewRecords instanceof ExternalSpillableMap)
          ? ((ExternalSpillableMap)keyToNewRecords).iterator() : keyToNewRecords.values().iterator();
      while (newRecordsItr.hasNext()) {
        HoodieRecord<T> hoodieRecord = newRecordsItr.next();
        if (!writtenRecordKeys.contains(hoodieRecord.getRecordKey())) {
          Schema schema = useWriterSchema ? inputSchemaWithMetaFields : inputSchema;
          Option<IndexedRecord> insertRecord =
              hoodieRecord.getData().getInsertValue(schema, config.getProps());
          // just skip the ignore record
          if (insertRecord.isPresent() && insertRecord.get() == IGNORE_RECORD) {
            continue;
          }
          writeRecord(hoodieRecord, insertRecord);
          insertRecordsWritten++;
        }
      }

      ((ExternalSpillableMap) keyToNewRecords).close();
      writtenRecordKeys.clear();

      if (fileWriter != null) {
        fileWriter.close();
      }

      long fileSizeInBytes = FSUtils.getFileSize(fs, newFilePath);
      HoodieWriteStat stat = writeStatus.getStat();

      stat.setTotalWriteBytes(fileSizeInBytes);
      stat.setFileSizeInBytes(fileSizeInBytes);
      stat.setNumWrites(recordsWritten);
      stat.setNumDeletes(recordsDeleted);
      stat.setNumUpdateWrites(updatedRecordsWritten);
      stat.setNumInserts(insertRecordsWritten);
      stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());
      RuntimeStats runtimeStats = new RuntimeStats();
      runtimeStats.setTotalUpsertTime(timer.endTimer());
      stat.setRuntimeStats(runtimeStats);

      performMergeDataValidationCheck(writeStatus);

      LOG.info(String.format("MergeHandle for partitionPath %s fileID %s, took %d ms.", stat.getPartitionPath(),
          stat.getFileId(), runtimeStats.getTotalUpsertTime()));

      return Collections.singletonList(writeStatus);
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to close UpdateHandle", e);
    }
  }

  public void performMergeDataValidationCheck(WriteStatus writeStatus) {
    if (!config.isMergeDataValidationCheckEnabled()) {
      return;
    }

    long oldNumWrites = 0;
    try {
      HoodieFileReader reader = HoodieFileReaderFactory.getFileReader(hoodieTable.getHadoopConf(), oldFilePath);
      oldNumWrites = reader.getTotalRecords();
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to check for merge data validation", e);
    }

    if ((writeStatus.getStat().getNumWrites() + writeStatus.getStat().getNumDeletes()) < oldNumWrites) {
      throw new HoodieCorruptedDataException(
          String.format("Record write count decreased for file: %s, Partition Path: %s (%s:%d + %d < %s:%d)",
              writeStatus.getFileId(), writeStatus.getPartitionPath(),
              instantTime, writeStatus.getStat().getNumWrites(), writeStatus.getStat().getNumDeletes(),
              FSUtils.getCommitTime(oldFilePath.toString()), oldNumWrites));
    }
  }

  public Path getOldFilePath() {
    return oldFilePath;
  }

  @Override
  public IOType getIOType() {
    return IOType.MERGE;
  }

  public HoodieBaseFile baseFileForMerge() {
    return baseFileToMerge;
  }
}
