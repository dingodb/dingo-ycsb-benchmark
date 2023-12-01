/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * DingoDB client binding for YCSB.
 *
 * All YCSB records are mapped to a DingoDB *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import io.dingodb.client.DingoClient;
import io.dingodb.client.common.Key;
import io.dingodb.client.common.Record;
import io.dingodb.client.common.Value;
import io.dingodb.common.Common;
import io.dingodb.sdk.common.partition.PartitionDetailDefinition;
import io.dingodb.sdk.common.partition.PartitionRule;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.TableDefinition;
import org.apache.commons.lang3.RandomStringUtils;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * YCSB binding for <a href="https://www.dingodb.com">DingoDB</a>.
 *
 * See {@code dingodb/README.md} for details.
 */
public class DingoDBClient extends DB {
  
  private static final AtomicInteger THREAD_COUNT = new AtomicInteger(0);

  /** The name of the property for the number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /** Default number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  /** Representing a NULL value. */
  public static final String NULL_VALUE = "NULL";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_PREFIX = "FIELD";

  /**
   * A singoton Dingo instance.
   */
  private static DingoClient dingoClient;

  /**
   * command for table such as create or drop table.
   */
  public static final String DINGO_TBL_COMMAND = "command";

  /**
   * create table.
   */
  public static final String DINGO_TBL_COMMAND_DEFAULT = "create";

  /**
   * drop table.
   */
  public static final String DINGO_TBL_COMMAND_DROP= "drop";

  /**
   * coordinator list for operation.
   */
  public static final String COORDINATOR_HOST = "coordinator.host";

  public static final String DINGO_TABLE_DEFAULT = "usertable";
  public static final String DINGO_TABLE = "dingo.table";

  private static TableDefinition tableDefinition;
  private static String defaultTableName;
  private static int columnCnt;

  @Override
  public void init() throws DBException {
    THREAD_COUNT.incrementAndGet();
    synchronized (THREAD_COUNT) {
      if (dingoClient != null) {
        return;
      }
      
      Properties props = getProperties();
      String coordinatorList = props.getProperty(COORDINATOR_HOST);
      String tableName = props.getProperty(DINGO_TABLE, DINGO_TABLE_DEFAULT);
      defaultTableName = tableName;
      
      dingoClient = new DingoClient(coordinatorList, 100);
      boolean isOK = dingoClient.open();
      if (!isOK) {
        throw new DBException("Init connection to coordinator:" + coordinatorList + " failed");
      }

      columnCnt = Integer.parseInt(
          props.getProperty(
              DingoDBClient.FIELD_COUNT_PROPERTY,
              DingoDBClient.FIELD_COUNT_PROPERTY_DEFAULT
          )
      );
      tableDefinition = getTableDefinition(defaultTableName);
      System.out.println("=======Init Input Table===================>>>>" + tableName);
    }
  }

  @Override
  public void cleanup() throws DBException {
    synchronized (THREAD_COUNT) {
      if (THREAD_COUNT.decrementAndGet() <= 0) {
        try {
          if (dingoClient != null) {
            dingoClient.close();
          }
        } catch (Exception e) {
          System.err.println("Could not close DingoDB connection pool: " + e.toString());
          e.printStackTrace();
        } finally {
          dingoClient = null;
        }
      }
    }
  }

  @Override
  public Status read(String tableName,
                     String key,
                     Set<String> fields,
                     Map<String, ByteIterator> result) {
    Record record = dingoClient.get(defaultTableName, new Key(Arrays.asList(Value.get(key))));
    Object[] values = record.getDingoColumnValuesInOrder();

    try {
      LinkedHashMap<String, String> resultInMap = convertRecord2HashMap(values);
      if (fields == null) {
        StringByteIterator.putAllAsByteIterators(result, resultInMap);
      } else {
        LinkedHashMap<String, String> subResultMap = new LinkedHashMap<>();
        for (String columnName: fields) {
          subResultMap.put(columnName, resultInMap.get(columnName.toLowerCase()));
        }
        StringByteIterator.putAllAsByteIterators(result, subResultMap);
      }
      return Status.OK;
    } catch (RuntimeException ex) {
      System.out.println("Catch exception:" + ex);
    }
    return Status.ERROR;
  }

  @Override
  public Status insert(String tableName,
                       String key,
                       Map<String, ByteIterator> values) {
    Map<String, String> inputValues = StringByteIterator.getStringMap(values);
//    LinkedHashMap<String, Object> map = Maps.newLinkedHashMap();
    try {
      TableDefinition tableDef = getTableDefinition(defaultTableName);
      List<Column> colList = tableDef.getColumns();
      List<Object> recordList = new ArrayList<>();
      for (Column column : colList) {
        String columnName = column.getName().toLowerCase();
        String columnValue = inputValues.get(columnName);
        if (columnValue == null) {
          columnValue = inputValues.get(columnName.toUpperCase());
        }
        
        if (columnName.equalsIgnoreCase(PRIMARY_KEY)) {
//          columnName = PRIMARY_KEY;
          columnValue = key;
        }
//        map.put(columnName, columnValue);
        recordList.add(columnValue);
      }
      
//      boolean isOK = dingoClient.upsert(defaultTableName, new Record(PRIMARY_KEY, map));
      boolean isOK = dingoClient.upsert(defaultTableName, new Record(colList, recordList));
      if (!isOK) {
        System.out.println("Insert record using key:[" + key + "], failed");
      }
    } catch (Exception ex) {
      System.out.println("Insert catch exception:" + ex);
      ex.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String tableName,
                       String key) {
    boolean isOK = dingoClient.delete(defaultTableName, new Key(Arrays.asList(Value.get(key))));
    if (!isOK) {
      System.out.println("delete record key:" + key + " failed");
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status update(String tableName,
                       String key,
                       Map<String, ByteIterator> values) {
    Map<String, String> inputValues = StringByteIterator.getStringMap(values);
    Record record = dingoClient.get(defaultTableName, new Key(Arrays.asList(Value.get(key))));
    
//    LinkedHashMap<String, Object> newRecordMap = Maps.newLinkedHashMap();
//    Object[] originRecord = record.getDingoColumnValuesInOrder();
//    List<Column> cl = tableDefinition.getColumns();
//    for (Map.Entry<String, String> entry: inputValues.entrySet()) {
//      for (int i = 0; i< cl.size(); i++) {
//        if (cl.get(i).getName().equalsIgnoreCase(entry.getKey())) { 
//          originRecord[i] = entry.getValue();
//        }
//        newRecordMap.put(cl.get(i).getName(), originRecord[i]);
//      }
//    }
//    
//    boolean isOK = dingoClient.upsert(
//        defaultTableName, 
//        new Record(PRIMARY_KEY, newRecordMap)
//    );
    
    Object[] originRecord = record.getDingoColumnValuesInOrder();
    List<Column> colList = tableDefinition.getColumns();
    List<Object> recordList = new ArrayList<>();
    
    for (Map.Entry<String, String> entry: inputValues.entrySet()) {
      for (int i = 0; i< colList.size(); i++) {
        if (colList.get(i).getName().equalsIgnoreCase(entry.getKey())) {
          originRecord[i] = entry.getValue();
        }
        recordList.add(originRecord[i]);
      }
    }
    
    boolean isOK = dingoClient.upsert(defaultTableName, new Record(colList, recordList));
    if (isOK) {
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String tableName,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.OK;
  }
  
  private static LinkedHashMap<String, String> convertRecord2HashMap(Object[] columnValues) {
    if (tableDefinition == null || tableDefinition.getColumns().size() == 0) {
      System.out.println("Invalid table definition:" + (tableDefinition == null ? "null" : "OK"));
      throw new RuntimeException("Table definition or Values is null");
    }
    
    LinkedHashMap<String, String> result = new LinkedHashMap<>();
    for (int i =0; i < columnValues.length; i++) {
      String columnName = tableDefinition.getColumn(i).getName();
      result.put(columnName.toLowerCase(), columnValues[i].toString());
    }
    return result;
  }

  private static TableDefinition getTableDefinition(String tableName) {

    /**
     * as the benchmark is on a single table, so the tableDefinition is only one.
     */
    if (tableDefinition == null) {
      List<Column> colDefList = new ArrayList<>();
      final String defaultTypeName = "varchar";
//      ColumnDefinition primaryColumn = new ColumnDefinition(
//          DingoDBClient.PRIMARY_KEY,
//          defaultTypeName,
//          "",
//          -1,
//          1,
//          false,
//          0,
//          generateRandomStr(20),
//          false
//      );
      ColumnDefinition primaryColumn = ColumnDefinition.builder()
          .name(DingoDBClient.PRIMARY_KEY)
          .type(defaultTypeName)
          .precision(-1)
          .scale(1)
          .nullable(false)
          .primary(0)
          .defaultValue(generateRandomStr(20))
          .isAutoIncrement(false)
          .build();
      colDefList.add(primaryColumn);
      
      for (int i = 0; i < columnCnt; i++) {
//        ColumnDefinition colDef = new ColumnDefinition(
//            DingoDBClient.COLUMN_PREFIX + i,
//            defaultTypeName,
//            "",
//            -1,
//            1,
//            true,
//            -1,
//            generateRandomStr(20),
//            false
//        );
        ColumnDefinition colDef = ColumnDefinition.builder()
            .name(DingoDBClient.COLUMN_PREFIX + i)
            .type(defaultTypeName)
            .precision(-1)
            .scale(1)
            .nullable(true)
            .primary(-1)
            .defaultValue(generateRandomStr(20))
            .isAutoIncrement(false)
            .build();
        colDefList.add(colDef);
      }

      PartitionDetailDefinition partitionDetailDefinition = new PartitionDetailDefinition(
          null, null, Arrays.asList(new Object[]{"a"}));
      PartitionRule partitionRule = new PartitionRule(
          null, null, Arrays.asList(partitionDetailDefinition));

//      tableDefinition = new TableDefinition(
//          tableName,
//          colDefList,
//          1,
//          0,
//          null,
//          Common.Engine.ENG_ROCKSDB.name(),
//          null
//      );
      tableDefinition = TableDefinition.builder()
          .name(tableName)
          .columns(colDefList)
          .version(1)
          .ttl(0)
          .partition(null)
          .engine(Common.Engine.ENG_ROCKSDB.name())
          .replica(3)
          .createSql("")
          .build();
    }
    return tableDefinition;
  }
  private static String generateRandomStr(int strLen) {
    return RandomStringUtils.randomAlphanumeric(strLen);
  }
}
