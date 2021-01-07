package org.apache.iotdb.db.engine.cache;

import static org.apache.iotdb.tsfile.utils.RamUsageEstimator.NUM_BYTES_INT;
import static org.apache.iotdb.tsfile.utils.RamUsageEstimator.NUM_BYTES_LONG;
import static org.apache.iotdb.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

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
    return NUM_BYTES_OBJECT_REF + NUM_BYTES_LONG + NUM_BYTES_INT;
  }
}
