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

package org.apache.iotdb.db.engine.cache;

import static org.apache.iotdb.tsfile.utils.RamUsageEstimator.NUM_BYTES_INT;
import static org.apache.iotdb.tsfile.utils.RamUsageEstimator.NUM_BYTES_LONG;
import static org.apache.iotdb.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;

public class FileStatistics {

  private long numOfPoints;
  private int sensorNum;

  public FileStatistics(long numOfPoints, int sensorNum) {
    this.numOfPoints = numOfPoints;
    this.sensorNum = sensorNum;
  }

  public long getNumOfPoints() {
    return numOfPoints;
  }

  public void setNumOfPoints(long numOfPoints) {
    this.numOfPoints = numOfPoints;
  }

  public int getSensorNum() {
    return sensorNum;
  }

  public void setSensorNum(int sensorNum) {
    this.sensorNum = sensorNum;
  }


  public long calculateRamSize() {
    return NUM_BYTES_OBJECT_HEADER + NUM_BYTES_LONG + NUM_BYTES_INT;
  }
}
