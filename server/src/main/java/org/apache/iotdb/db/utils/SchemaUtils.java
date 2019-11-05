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
package org.apache.iotdb.db.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;


public class SchemaUtils {

  private SchemaUtils(){}

  /**
   * Construct the Schema of the FileNode named processorName.
   * @param processorName the name of a FileNode.
   * @return the schema of the FileNode named processorName.
   * @throws WriteProcessException when the fileSchema cannot be created.
   */
  public static Schema constructSchema(String processorName) {
    List<MeasurementSchema> columnSchemaList;
    columnSchemaList = MManager.getInstance().getSchemaForStorageGroup(processorName);
    return getSchemaFromColumnSchema(columnSchemaList);
  }

  /**
   * getSchemaFromColumnSchema construct a Schema using the schema of the columns and the
   * device type.
   * @param schemaList the schema of the columns in this file.
   * @return a Schema contains the provided schemas.
   */
  private static Schema getSchemaFromColumnSchema(List<MeasurementSchema> schemaList) {
    Schema schema = new Schema();
    for (MeasurementSchema measurementSchema : schemaList) {
      schema.registerMeasurement(measurementSchema);
    }
    return schema;
  }

  /**
   * combine multiple metadata in string format
   */
  public static String combineMetadataInStrings(String[] metadataStrs) {
    JSONObject[] jsonObjects = new JSONObject[metadataStrs.length];
    for (int i = 0; i < jsonObjects.length; i++) {
      jsonObjects[i] = JSONObject.parseObject(metadataStrs[i]);
    }

    JSONObject root = jsonObjects[0];
    for (int i = 1; i < jsonObjects.length; i++) {
      root = combineJSONObjects(root, jsonObjects[i]);
    }
    return jsonToString(root);
  }

  public static String jsonToString(JSONObject jsonObject) {
    return JSON.toJSONString(jsonObject, SerializerFeature.PrettyFormat);
  }

  private static JSONObject combineJSONObjects(JSONObject a, JSONObject b) {
    JSONObject res = new JSONObject();

    Set<String> retainSet = new HashSet<>(a.keySet());
    retainSet.retainAll(b.keySet());
    Set<String> aCha = new HashSet<>(a.keySet());
    Set<String> bCha = new HashSet<>(b.keySet());
    aCha.removeAll(retainSet);
    bCha.removeAll(retainSet);
    for (String key : aCha) {
      res.put(key, a.getJSONObject(key));
    }
    for (String key : bCha) {
      res.put(key, b.get(key));
    }
    for (String key : retainSet) {
      Object v1 = a.get(key);
      Object v2 = b.get(key);
      if (v1 instanceof JSONObject && v2 instanceof JSONObject) {
        res.put(key, combineJSONObjects((JSONObject) v1, (JSONObject) v2));
      } else {
        res.put(key, v1);
      }
    }
    return res;
  }
}
