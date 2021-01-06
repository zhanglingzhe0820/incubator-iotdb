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

package org.apache.iotdb.db.query.udf.datastructure.row;

import static org.apache.iotdb.db.conf.IoTDBConstant.MB;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.datastructure.SerializableList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class SerializableRowRecordList implements SerializableList {

  protected static final int MIN_OBJECT_HEADER_SIZE = 8;
  protected static final int MIN_ARRAY_HEADER_SIZE = MIN_OBJECT_HEADER_SIZE + 4;

  public static SerializableRowRecordList newSerializableRowRecordList(TSDataType[] dataTypes,
      long queryId) {
    SerializationRecorder recorder = new SerializationRecorder(queryId);
    return new SerializableRowRecordList(dataTypes, recorder);
  }

  protected static int calculateCapacity(TSDataType[] dataTypes, float memoryLimitInMB,
      int byteArrayLengthForMemoryControl) throws QueryProcessException {
    int rowLength = ReadWriteIOUtils.LONG_LEN; // timestamp
    for (TSDataType dataType : dataTypes) { // fields
      switch (dataType) {
        case INT32:
          rowLength += ReadWriteIOUtils.INT_LEN;
          break;
        case INT64:
          rowLength += ReadWriteIOUtils.LONG_LEN;
          break;
        case FLOAT:
          rowLength += ReadWriteIOUtils.FLOAT_LEN;
          break;
        case DOUBLE:
          rowLength += ReadWriteIOUtils.DOUBLE_LEN;
          break;
        case BOOLEAN:
          rowLength += ReadWriteIOUtils.BOOLEAN_LEN;
          break;
        case TEXT:
          rowLength += MIN_OBJECT_HEADER_SIZE + MIN_ARRAY_HEADER_SIZE
              + byteArrayLengthForMemoryControl;
          break;
        default:
          throw new UnSupportedDataTypeException(dataType.toString());
      }
    }

    int size = (int) (memoryLimitInMB * MB / 2 / rowLength);
    if (size <= 0) {
      throw new QueryProcessException("Memory is not enough for current query.");
    }
    return size;
  }

  private final TSDataType[] dataTypes;
  private final SerializationRecorder serializationRecorder;

  private List<RowRecord> rowRecords;

  private SerializableRowRecordList(TSDataType[] dataTypes,
      SerializationRecorder serializationRecorder) {
    this.dataTypes = dataTypes;
    this.serializationRecorder = serializationRecorder;
    init();
  }

  public boolean isEmpty() {
    return rowRecords.isEmpty();
  }

  public int size() {
    return rowRecords.size();
  }

  public RowRecord getRowRecord(int index) {
    return rowRecords.get(index);
  }

  public long getTime(int index) {
    return rowRecords.get(index).getTimestamp();
  }

  public void put(RowRecord rowRecord) {
    rowRecords.add(rowRecord);
  }

  @Override
  public void release() {
    rowRecords = null;
  }

  @Override
  public void init() {
    rowRecords = new ArrayList<>();
  }

  @Override
  public void serialize(PublicBAOS outputStream) throws IOException {
    int size = rowRecords.size();
    serializationRecorder.setSerializedElementSize(size);
    int serializedByteLength = 0;
    int nullCount = 0;
    for (RowRecord record : rowRecords) {
      if (record != null) {
        break;
      }
      ++nullCount;
    }
    serializedByteLength += ReadWriteIOUtils.write(nullCount, outputStream);
    for (int i = nullCount; i < size; ++i) {
      RowRecord rowRecord = rowRecords.get(i);
      serializedByteLength += ReadWriteIOUtils.write(rowRecord.getTimestamp(), outputStream);
      serializedByteLength += writeFields(rowRecord, outputStream);
    }
    serializationRecorder.setSerializedByteLength(serializedByteLength);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    int serializedElementSize = serializationRecorder.getSerializedElementSize();
    int nullCount = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < nullCount; ++i) {
      put(null);
    }
    for (int i = nullCount; i < serializedElementSize; ++i) {
      long timestamp = ReadWriteIOUtils.readLong(byteBuffer);
      List<Field> fields = readFields(byteBuffer);
      put(new RowRecord(timestamp, fields));
    }
  }

  private int writeFields(RowRecord rowRecord, PublicBAOS outputStream) throws IOException {
    int serializedByteLength = 0;
    List<Field> fields = rowRecord.getFields();
    for (int i = 0; i < dataTypes.length; ++i) {
      Field field = fields.get(i);
      boolean isNull = field == null;
      serializedByteLength += ReadWriteIOUtils.write(isNull, outputStream);
      if (isNull) {
        continue;
      }

      switch (dataTypes[i]) {
        case INT32:
          serializedByteLength += ReadWriteIOUtils.write(field.getIntV(), outputStream);
          break;
        case INT64:
          serializedByteLength += ReadWriteIOUtils.write(field.getLongV(), outputStream);
          break;
        case FLOAT:
          serializedByteLength += ReadWriteIOUtils.write(field.getFloatV(), outputStream);
          break;
        case DOUBLE:
          serializedByteLength += ReadWriteIOUtils.write(field.getDoubleV(), outputStream);
          break;
        case BOOLEAN:
          serializedByteLength += ReadWriteIOUtils.write(field.getBoolV(), outputStream);
          break;
        case TEXT:
          serializedByteLength += ReadWriteIOUtils.write(field.getBinaryV(), outputStream);
          break;
        default:
          throw new UnSupportedDataTypeException(dataTypes[i].toString());
      }
    }
    return serializedByteLength;
  }

  private List<Field> readFields(ByteBuffer byteBuffer) {
    List<Field> fields = new ArrayList<>();
    for (TSDataType dataType : dataTypes) {
      boolean isNull = ReadWriteIOUtils.readBool(byteBuffer);
      if (isNull) {
        fields.add(null);
        continue;
      }

      Field field;
      switch (dataType) {
        case INT32:
          field = new Field(TSDataType.INT32);
          field.setIntV(ReadWriteIOUtils.readInt(byteBuffer));
          break;
        case INT64:
          field = new Field(TSDataType.INT64);
          field.setLongV(ReadWriteIOUtils.readLong(byteBuffer));
          break;
        case FLOAT:
          field = new Field(TSDataType.FLOAT);
          field.setFloatV(ReadWriteIOUtils.readFloat(byteBuffer));
          break;
        case DOUBLE:
          field = new Field(TSDataType.DOUBLE);
          field.setDoubleV(ReadWriteIOUtils.readDouble(byteBuffer));
          break;
        case BOOLEAN:
          field = new Field(TSDataType.BOOLEAN);
          field.setBoolV(ReadWriteIOUtils.readBool(byteBuffer));
          break;
        case TEXT:
          field = new Field(TSDataType.TEXT);
          field.setBinaryV(ReadWriteIOUtils.readBinary(byteBuffer));
          break;
        default:
          throw new UnSupportedDataTypeException(dataType.toString());
      }
      fields.add(field);
    }
    return fields;
  }

  @Override
  public SerializationRecorder getSerializationRecorder() {
    return serializationRecorder;
  }
}
