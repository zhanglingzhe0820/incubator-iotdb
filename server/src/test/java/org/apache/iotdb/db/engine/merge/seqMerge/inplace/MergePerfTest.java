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

package org.apache.iotdb.db.engine.merge.seqMerge.inplace;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.MergeTest;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.seqMerge.inplace.task.InplaceMergeTask;
import org.apache.iotdb.tsfile.read.common.util.ChunkProviderExecutor;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class MergePerfTest extends MergeTest {

    private boolean fullMerge;

    public void test() throws Exception {
        MManager.getInstance().init();
        File tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
        tempSGDir.mkdirs();
        setUp();
        System.out.println("Files prepared.");
        Thread.sleep(3000);

        long timeConsumption = System.currentTimeMillis();
        MergeResource resource = new MergeResource(seqResources, unseqResources);
        resource.setCacheDeviceMeta(true);
        InplaceMergeTask mergeTask =
                new InplaceMergeTask(resource, tempSGDir.getPath(), (k, v
                        , l, n) -> {
                }, "test", fullMerge, 100, MERGE_TEST_SG);
        mergeTask.call();
        timeConsumption = System.currentTimeMillis() - timeConsumption;
        tearDown();
        ChunkProviderExecutor.getINSTANCE().close();
        FileUtils.deleteDirectory(tempSGDir);
        System.out.println(timeConsumption);
    }

    public static void main(String[] args) throws Exception {
        IoTDBDescriptor.getInstance().getConfig().setChunkMergePointThreshold(-1);

        List<Long> timeConsumptions = new ArrayList<>();
        MergePerfTest perfTest = new MergePerfTest();

        perfTest.seqFileNum = 5;
        perfTest.unseqFileNum = 5;
        perfTest.measurementNum = 2000;
        perfTest.deviceNum = 1;
        perfTest.ptNum = 5000;
        perfTest.flushInterval = 1000;
        perfTest.fullMerge = true;
        perfTest.encoding = TSEncoding.PLAIN;

        for (int i = 0; i < 1; i++) {
            // cache warm-up
            perfTest.test();
        }

//    int[] intParameters = new int[10];
//    for (int i = 1; i <= 10; i++) {
//      intParameters[i-1] = i;
//    }
//    for (int param : intParameters) {
//      perfTest.unseqFileNum = param;
//      perfTest.test();
//      timeConsumptions.add(perfTest.timeConsumption);
//    }
//    long[] longParameters = new long[10];
//    for (int i = 1; i <= 10; i++) {
//      longParameters[i-1] = i * 200;
//    }
//    for (long param : longParameters) {
//      perfTest.flushInterval = param;
//      perfTest.test();
//      timeConsumptions.add(perfTest.timeConsumption);
//    }
//    double[] doubleParameters = new double[10];
//    for (int i = 1; i <= 10; i++) {
//      doubleParameters[i-1] = 0.1 * i;
//    }
//    for (double param : doubleParameters) {
//      perfTest.unseqRatio = param;
//      perfTest.test();
//      timeConsumptions.add(perfTest.timeConsumption);
//    }

        System.out.println(timeConsumptions);
    }
}
