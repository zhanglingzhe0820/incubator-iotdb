/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.iotdb.db.engine.storagegroup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TTLTest {

  private String sg1 = "root.TTL_SG1";
  private String sg2 = "root.TTL_SG2";
  private long ttl = 12345;
  private StorageGroupProcessor storageGroupProcessor;
  private String s1 = "s1";
  private String g1s1 = sg1 + IoTDBConstant.PATH_SEPARATOR + s1;
  private long prevPartitionInterval;

  @Before
  public void setUp()
      throws MetadataException, IOException, StartupException, StorageGroupProcessorException {
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(86400);
    EnvironmentUtils.envSetUp();
    createSchemas();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    storageGroupProcessor.syncCloseAllWorkingTsFileProcessors();
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(prevPartitionInterval);
  }

  private void createSchemas()
      throws MetadataException, StorageGroupProcessorException {
    MManager.getInstance().setStorageGroup(sg1);
    MManager.getInstance().setStorageGroup(sg2);
    storageGroupProcessor = new StorageGroupProcessor(IoTDBDescriptor.getInstance().getConfig()
        .getSystemDir(), sg1, new DirectFlushPolicy());
    MManager.getInstance().createTimeseries(g1s1, TSDataType.INT64, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, Collections.emptyMap());
  }

  @Test
  public void testSetMetaTTL() throws IOException, MetadataException {
    // exception is expected when setting ttl to a non-exist storage group
    boolean caught = false;

    try {
      MManager.getInstance().setTTL(sg1 + ".notExist", ttl);
    } catch (MetadataException e) {
      caught = true;
    }
    assertTrue(caught);

    // normally set ttl
    MManager.getInstance().setTTL(sg1, ttl);
    StorageGroupMNode mNode = MManager.getInstance().getStorageGroupNode(sg1);
    assertEquals(ttl, mNode.getDataTTL());

    // default ttl
    mNode = MManager.getInstance().getStorageGroupNode(sg2);
    assertEquals(Long.MAX_VALUE, mNode.getDataTTL());
  }

  @Test
  public void testTTLWrite() throws WriteProcessException, QueryProcessException {
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(sg1);
    insertPlan.setTime(System.currentTimeMillis());
    insertPlan.setMeasurements(new String[]{"s1"});
    insertPlan.setTypes(new TSDataType[]{TSDataType.INT64});
    insertPlan.setValues(new Object[]{1L});
    insertPlan.setSchemasAndTransferType(
        new MeasurementSchema[]{new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN)});

    // ok without ttl
    storageGroupProcessor.insert(insertPlan);

    storageGroupProcessor.setDataTTL(1000);
    // with ttl
    insertPlan.setTime(System.currentTimeMillis() - 1001);
    boolean caught = false;
    try {
      storageGroupProcessor.insert(insertPlan);
    } catch (OutOfTTLException e) {
      caught = true;
    }
    assertTrue(caught);
    insertPlan.setTime(System.currentTimeMillis() - 900);
    storageGroupProcessor.insert(insertPlan);
  }

  private void prepareData() throws WriteProcessException, QueryProcessException {
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(sg1);
    insertPlan.setTime(System.currentTimeMillis());
    insertPlan.setMeasurements(new String[]{"s1"});
    insertPlan.setTypes(new TSDataType[]{TSDataType.INT64});
    insertPlan.setValues(new Object[]{1L});
    insertPlan.setSchemasAndTransferType(
        new MeasurementSchema[]{new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN)});

    long initTime = System.currentTimeMillis();
    // sequence data
    for (int i = 1000; i < 2000; i++) {
      insertPlan.setTime(initTime - 2000 + i);
      storageGroupProcessor.insert(insertPlan);
      if ((i + 1) % 300 == 0) {
        storageGroupProcessor.syncCloseAllWorkingTsFileProcessors();
      }
    }
    // unsequence data
    for (int i = 0; i < 1000; i++) {
      insertPlan.setTime(initTime - 2000 + i);
      storageGroupProcessor.insert(insertPlan);
      if ((i + 1) % 300 == 0) {
        storageGroupProcessor.syncCloseAllWorkingTsFileProcessors();
      }
    }
  }

  @Test
  public void testTTLRead()
      throws IOException, WriteProcessException, StorageEngineException, QueryProcessException {
    prepareData();

    // files before ttl
    QueryDataSource dataSource = storageGroupProcessor
        .query(sg1, s1, EnvironmentUtils.TEST_QUERY_CONTEXT, null, null);
    List<TsFileResource> seqResource = dataSource.getSeqResources();
    List<TsFileResource> unseqResource = dataSource.getUnseqResources();
    assertEquals(4, seqResource.size());
    assertEquals(4, unseqResource.size());

    storageGroupProcessor.setDataTTL(500);

    // files after ttl
    dataSource = storageGroupProcessor
        .query(sg1, s1, EnvironmentUtils.TEST_QUERY_CONTEXT, null, null);
    seqResource = dataSource.getSeqResources();
    unseqResource = dataSource.getUnseqResources();
    assertTrue(seqResource.size() < 4);
    assertEquals(0, unseqResource.size());
    Path path = new Path(sg1, s1);
    Set<String> allSensors = new HashSet<>();
    allSensors.add(s1);
    IBatchReader reader = new SeriesRawDataBatchReader(path, allSensors, TSDataType.INT64,
        EnvironmentUtils.TEST_QUERY_CONTEXT, dataSource, null, null, null);

    int cnt = 0;
    while (reader.hasNextBatch()) {
      BatchData batchData = reader.nextBatch();
      while (batchData.hasCurrent()) {
        batchData.next();
        cnt++;
      }
    }
    reader.close();
    // we cannot offer the exact number since when exactly ttl will be checked is unknown
    assertTrue(cnt <= 1000);

    storageGroupProcessor.setDataTTL(0);
    dataSource = storageGroupProcessor.query(sg1, s1, EnvironmentUtils.TEST_QUERY_CONTEXT
        , null, null);
    seqResource = dataSource.getSeqResources();
    unseqResource = dataSource.getUnseqResources();
    assertEquals(0, seqResource.size());
    assertEquals(0, unseqResource.size());

    QueryResourceManager.getInstance().endQuery(EnvironmentUtils.TEST_QUERY_JOB_ID);
  }

  @Test
  public void testTTLRemoval()
      throws StorageEngineException, WriteProcessException, QueryProcessException {
    prepareData();

    storageGroupProcessor.syncCloseAllWorkingTsFileProcessors();

    // files before ttl
    File seqDir = new File(DirectoryManager.getInstance().getNextFolderForSequenceFile(), sg1);
    File unseqDir = new File(DirectoryManager.getInstance().getNextFolderForUnSequenceFile(), sg1);

    List<File> seqFiles = new ArrayList<>();
    for (File directory : seqDir.listFiles()) {
      if (directory.isDirectory()) {
        for (File file : directory.listFiles()) {
          if (file.getPath().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
            seqFiles.add(file);
          }
        }
      }
    }

    List<File> unseqFiles = new ArrayList<>();
    for (File directory : unseqDir.listFiles()) {
      if (directory.isDirectory()) {
        for (File file : directory.listFiles()) {
          if (file.getPath().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
            unseqFiles.add(file);
          }
        }
      }
    }

    assertEquals(4, seqFiles.size());
    assertEquals(4, unseqFiles.size());

    storageGroupProcessor.setDataTTL(500);
    storageGroupProcessor.checkFilesTTL();

    // files after ttl
    seqFiles = new ArrayList<>();
    for (File directory : seqDir.listFiles()) {
      if (directory.isDirectory()) {
        for (File file : directory.listFiles()) {
          if (file.getPath().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
            seqFiles.add(file);
          }
        }
      }
    }

    unseqFiles = new ArrayList<>();
    for (File directory : unseqDir.listFiles()) {
      if (directory.isDirectory()) {
        for (File file : directory.listFiles()) {
          if (file.getPath().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
            unseqFiles.add(file);
          }
        }
      }
    }

    assertTrue(seqFiles.size() <= 2);
    assertEquals(0, unseqFiles.size());
  }

  @Test
  public void testParseSetTTL() throws QueryProcessException {
    Planner planner = new Planner();
    SetTTLPlan plan = (SetTTLPlan) planner
        .parseSQLToPhysicalPlan("SET TTL TO " + sg1 + " 10000");
    assertEquals(sg1, plan.getStorageGroup());
    assertEquals(10000, plan.getDataTTL());

    plan = (SetTTLPlan) planner.parseSQLToPhysicalPlan("UNSET TTL TO " + sg2);
    assertEquals(sg2, plan.getStorageGroup());
    assertEquals(Long.MAX_VALUE, plan.getDataTTL());
  }

  @Test
  public void testParseShowTTL() throws QueryProcessException {
    Planner planner = new Planner();
    ShowTTLPlan plan = (ShowTTLPlan) planner.parseSQLToPhysicalPlan("SHOW ALL TTL");
    assertTrue(plan.getStorageGroups().isEmpty());

    List<String> sgs = new ArrayList<>();
    sgs.add("root.sg1");
    sgs.add("root.sg2");
    sgs.add("root.sg3");
    plan = (ShowTTLPlan) planner
        .parseSQLToPhysicalPlan("SHOW TTL ON root.sg1,root.sg2,root.sg3");
    assertEquals(sgs, plan.getStorageGroups());
  }

  @Test
  public void testShowTTL()
      throws IOException, QueryProcessException, QueryFilterOptimizationException,
      StorageEngineException, MetadataException, TException, InterruptedException {
    MManager.getInstance().setTTL(sg1, ttl);

    ShowTTLPlan plan = new ShowTTLPlan(Collections.emptyList());
    PlanExecutor executor = new PlanExecutor();
    QueryDataSet queryDataSet = executor.processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    RowRecord rowRecord = queryDataSet.next();
    assertEquals(sg1, rowRecord.getFields().get(0).getStringValue());
    assertEquals(ttl, rowRecord.getFields().get(1).getLongV());

    rowRecord = queryDataSet.next();
    assertEquals(sg2, rowRecord.getFields().get(0).getStringValue());
    assertNull(rowRecord.getFields().get(1));
  }

  @Test
  public void testTTLCleanFile() throws WriteProcessException, QueryProcessException {
    prepareData();
    storageGroupProcessor.syncCloseAllWorkingTsFileProcessors();

    assertEquals(4, storageGroupProcessor.getSequenceFileTreeSet().size());
    assertEquals(4, storageGroupProcessor.getUnSequenceFileList().size());

    storageGroupProcessor.setDataTTL(0);
    assertEquals(0, storageGroupProcessor.getSequenceFileTreeSet().size());
    assertEquals(0, storageGroupProcessor.getUnSequenceFileList().size());
  }
}