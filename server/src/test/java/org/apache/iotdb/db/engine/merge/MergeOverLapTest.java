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
 */

package org.apache.iotdb.db.engine.merge;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Callable;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.merge.seqMerge.inplace.task.InplaceMergeTask;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.seqMerge.squeeze.task.SqueezeMergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceIterateReader;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MergeOverLapTest extends MergeTest {

  private File tempSGDir;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, PathException {
    ptNum = 1000;
    super.setUp();
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  @Override
  protected void prepareFiles(int seqFileNum, int unseqFileNum) throws IOException, WriteProcessException {
    for (int i = 0; i < seqFileNum; i++) {
      File file = new File(TestConstant.BASE_OUTPUT_PATH.concat(
          i + "seq" + IoTDBConstant.TSFILE_NAME_SEPARATOR + i + IoTDBConstant.TSFILE_NAME_SEPARATOR
              + i + IoTDBConstant.TSFILE_NAME_SEPARATOR + 0
              + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }
    for (int i = 0; i < unseqFileNum; i++) {
      File file = new File(TestConstant.BASE_OUTPUT_PATH.concat(
              i + "unseq" + IoTDBConstant.TSFILE_NAME_SEPARATOR + i + IoTDBConstant.TSFILE_NAME_SEPARATOR
              + i + IoTDBConstant.TSFILE_NAME_SEPARATOR + 0
              + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      unseqResources.add(tsFileResource);
      prepareUnseqFile(tsFileResource, i * ptNum, ptNum * (i + 1) / unseqFileNum, 10000);
    }
    File file = new File(TestConstant.BASE_OUTPUT_PATH.concat(
            unseqFileNum + "unseq" + IoTDBConstant.TSFILE_NAME_SEPARATOR + unseqFileNum
            + IoTDBConstant.TSFILE_NAME_SEPARATOR + unseqFileNum + IoTDBConstant.TSFILE_NAME_SEPARATOR + 0
            + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    unseqResources.add(tsFileResource);
    prepareUnseqFile(tsFileResource, 0, ptNum * unseqFileNum, 20000);
  }

  private void prepareUnseqFile(TsFileResource tsFileResource, long timeOffset, long ptNum,
      long valueOffset)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getFile());
    for (MeasurementSchema measurementSchema : measurementSchemas) {
      fileWriter.addMeasurement(measurementSchema);
    }
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(i, deviceIds[j]);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(DataPoint.getDataPoint(measurementSchemas[k].getType(),
              measurementSchemas[k].getMeasurementId(), String.valueOf(i + valueOffset)));
        }
        fileWriter.write(record);
        tsFileResource.updateStartTime(deviceIds[j], i);
        tsFileResource.updateEndTime(deviceIds[j], i);
      }
      // insert overlapping tuples
      if ((i + 1) % 100 == 0) {
        for (int j = 0; j < deviceNum; j++) {
          TSRecord record = new TSRecord(i, deviceIds[j]);
          for (int k = 0; k < measurementNum; k++) {
            record.addTuple(DataPoint.getDataPoint(measurementSchemas[k].getType(),
                measurementSchemas[k].getMeasurementId(), String.valueOf(i + valueOffset)));
          }
          fileWriter.write(record);
          tsFileResource.updateStartTime(deviceIds[j], i);
          tsFileResource.updateEndTime(deviceIds[j], i);
        }
      }
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushForTest();
      }
    }
    fileWriter.close();
  }

  @Test
  public void testInplaceFullMerge() throws Exception {
    Callable mergeTask =
        new InplaceMergeTask(new MergeResource(seqResources, unseqResources), tempSGDir.getPath(),
            (k, v, l, n) -> {}, "test",
            true, 1, MERGE_TEST_SG);
    mergeTask.call();
    check(seqResources.get(0), 1000);
  }

  @Test
  public void testSqueezeFullMerge() throws Exception {
    TsFileResource[] newResource = new TsFileResource[1];
    Callable mergeTask =
        new SqueezeMergeTask(new MergeResource(seqResources, unseqResources), tempSGDir.getPath(),
            (k, v, l, n) -> newResource[0] = n, "test", 1, MERGE_TEST_SG);
    mergeTask.call();
    check(newResource[0], 5000);
    newResource[0].close();
    newResource[0].remove();
  }


  private void check(TsFileResource mergedFile, long expected) throws IOException {
    QueryContext context = new QueryContext();
    Path path = new Path(deviceIds[0], measurementSchemas[0].getMeasurementId());
    SeqResourceIterateReader tsFilesReader = new SeqResourceIterateReader(path,
        Collections.singletonList(mergedFile), null, context);
    int cnt = 0;
    try {
      while (tsFilesReader.hasNextBatch()) {
        BatchData batchData = tsFilesReader.nextBatch();
        for (int i = 0; i < batchData.length(); i++) {
          cnt ++;
          assertEquals(batchData.getTimeByIndex(i) + 20000.0, batchData.getDoubleByIndex(i), 0.001);
        }
      }
      assertEquals(expected, cnt);
    } finally {
      tsFilesReader.close();
    }
  }
}
