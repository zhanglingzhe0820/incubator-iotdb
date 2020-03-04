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

package org.apache.iotdb.db.engine.merge.sizeMerge.simple.selector;

import org.apache.iotdb.db.engine.merge.BaseFileSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.seqmerge.inplace.selector.InplaceMaxFileSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * MaxFileMergeFileSelector selects the most files from given seqFiles and unseqFiles which can be
 * merged without exceeding given memory budget. It always assume the number of timeseries being
 * queried at the same time is 1 to maximize the number of file merged.
 */
public class SimpleMaxFileSelector extends BaseFileSelector {

    private static final Logger logger = LoggerFactory.getLogger(
            SimpleMaxFileSelector.class);

    public SimpleMaxFileSelector(MergeResource resource, long memoryBudget) {
        this.resource = resource;
        this.memoryBudget = memoryBudget;
    }

    public void select(boolean useTightBound) throws IOException {
        for (int i = 0; i < resource.getSeqFiles().size() - 1 && timeConsumption < timeLimit; i++) {
            // try to find candidates starting from i
            TsFileResource seqFile = resource.getSeqFiles().get(i);
            logger.debug("Try selecting seq file {}/{}, {}", i, resource.getSeqFiles().size() - 1, seqFile);
            long fileCost = calculateSeqFileCost(seqFile, useTightBound);
            if (fileCost < memoryBudget) {
                totalCost = fileCost;
                logger.debug("Seq file {} can fit memory, search from it", seqFile);
                selectedSeqFiles.add(resource.getSeqFiles().get(i));
            } else {
                logger.debug("File {} cannot fie memory {}/{}", seqFile, fileCost, memoryBudget);
            }
            timeConsumption = System.currentTimeMillis() - startTime;
        }
    }

    private long calculateSeqFileCost(TsFileResource seqFile, boolean useTightBound)
            throws IOException {
        long fileCost = 0;
        long fileReadCost = useTightBound ? calculateTightSeqMemoryCost(seqFile) :
                calculateMetadataSize(seqFile);
        logger.debug("File read cost of {} is {}", seqFile, fileReadCost);
        if (fileReadCost > tempMaxSeqFileCost) {
            // memory used when read data from a seq file:
            // only one file will be read at the same time, so only the largest one is recorded here
            fileCost -= tempMaxSeqFileCost;
            fileCost += fileReadCost;
            tempMaxSeqFileCost = fileReadCost;
        }
        // memory used to cache the metadata before the new file is closed
        // but writing data into a new file may generate the same amount of metadata in memory
        fileCost += calculateMetadataSize(seqFile);
        logger.debug("File cost of {} is {}", seqFile, fileCost);
        return fileCost;
    }

    protected void updateCost(long newCost, TsFileResource unseqFile) {
    }

    protected void selectOverlappedSeqFiles(TsFileResource unseqFile) {
    }
}
