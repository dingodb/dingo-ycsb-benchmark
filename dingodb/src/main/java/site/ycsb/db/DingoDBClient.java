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

import io.dingodb.common.DingoOpResult;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.sdk.client.DingoClient;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.operation.Value;
import io.dingodb.sdk.operation.op.Op;
import io.dingodb.sdk.operation.result.CollectionOpResult;
import site.ycsb.*;

import java.util.*;

/**
 * YCSB binding for <a href="https://www.dingodb.com">DingoDB</a>.
 *
 * See {@code dingodb/README.md} for details.
 */
public class DingoDBClient extends DB {

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

  private DingoClient   dingoClient;

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
  private String defaultTableName;

  private static final String DINGO_USER = System.getenv("DINGO_USER");
  private static final  String DINGO_PWD = System.getenv("DINGO_PWD");


  public void init() throws DBException {
    Properties props = getProperties();
    String coordinatorList = props.getProperty(COORDINATOR_HOST);
    String tableName = props.getProperty(DINGO_TABLE, DINGO_TABLE_DEFAULT);
    defaultTableName = tableName;

    dingoClient = new DingoClient(coordinatorList);
    dingoClient.setIdentity(DINGO_USER, DINGO_PWD);
    boolean isOK = dingoClient.open();
    if (!isOK) {
      throw new DBException("Init connection to coordinator:" + coordinatorList + " failed");
    }
    tableDefinition = dingoClient.getTableDefinition(tableName);
    System.out.println("=======Init Input Table===================>>>>" + tableName);
  }

  public void cleanup() throws DBException {
    dingoClient.close();
  }

  @Override
  public Status read(String tableName,
                     String key,
                     Set<String> fields,
                     Map<String, ByteIterator> result) {
//    Object[] keyArray = new Object[1];
//    keyArray[0] = key;
//    Object[] values = dingoClient.get(defaultTableName, keyArray);
    Key key1=new Key(tableName, new ArrayList<>(Arrays.asList(Value.get(key))));
    Op getRecord= Op.get(key1);
    DingoOpResult dingoOpResult = dingoClient.exec(getRecord);

    Iterator<Object[]> value = ((CollectionOpResult<Iterator<Object[]>>) dingoOpResult).getValue();


    Object[] values=value.next();

    try {
      Map<String, String> resultInMap = convertRecord2HashMap(dingoClient, defaultTableName, values);
      if (fields == null) {
        StringByteIterator.putAllAsByteIterators(result, resultInMap);
      } else {
        Map<String, String> subResultMap = new HashMap<>();
        for (String columnName: fields) {
          System.err.println("读结果是======="+resultInMap.get(columnName.toLowerCase()));
          subResultMap.put(columnName, resultInMap.get(columnName.toLowerCase()));
        }
        StringByteIterator.putAllAsByteIterators(result, subResultMap);
      }
      return Status.OK;
    } catch (RuntimeException ex) {
      System.out.println("Catch exception:" + ex.toString());
    }
    return Status.ERROR;
  }

  @Override
  public Status insert(String tableName,
                       String key,
                       Map<String, ByteIterator> values) {
    Map<String, String> inputValues = StringByteIterator.getStringMap(values);
    Object[] resultArray = new Object[values.size() + 1];

    try {
      TableDefinition tableDef = getTableDefinition(dingoClient, defaultTableName);
      int index = 0;
      for (ColumnDefinition column : tableDef.getColumns()) {
        String columnName = column.getName().toLowerCase();
        String columnValue = inputValues.get(columnName);
        if (columnValue == null) {
          columnValue = inputValues.get(columnName.toUpperCase());
        }

        if (columnName.equalsIgnoreCase(PRIMARY_KEY)) {
          columnValue = key;
        }
        resultArray[index] = columnValue;
        index++;
      }

      boolean isOK = dingoClient.insert(defaultTableName, resultArray);
      if (!isOK) {
        System.out.println("Insert record using key:[" + key + "], failed");
      }
    } catch (Exception ex) {
      System.out.println("Insert catch exception:" + ex.toString());
      ex.printStackTrace();
      return Status.ERROR;
    }
//    System.err.println("插入数据成功");
    return Status.OK;
  }

  @Override
  public Status delete(String tableName,
                       String key) {
    boolean isOK = dingoClient.delete(defaultTableName, Arrays.asList(key).toArray());
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

    Key key1=new Key(tableName, new ArrayList<>(Arrays.asList(Value.get(key))));
    Op getRecord= Op.get(key1);
    DingoOpResult dingoOpResult = dingoClient.exec(getRecord);

    Iterator<Object[]> value = ((CollectionOpResult<Iterator<Object[]>>) dingoOpResult).getValue();


    Object[] originRecord=value.next();

//    Object[] originRecord = dingoClient.get(defaultTableName, Arrays.asList(key).toArray());
    for (Map.Entry<String, String> entry: inputValues.entrySet()) {
      int index = tableDefinition.getColumnIndex(entry.getKey());
      if (index != -1) {
        originRecord[index] = entry.getValue();
      }
    }

    boolean isOK =  dingoClient.insert(defaultTableName, originRecord);
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


//    Key startKey = new Key(tableName, Arrays.asList(Value.get(startkey)));

    return Status.OK;
  }

  private static Map<String, String> convertRecord2HashMap(DingoClient client,
                                                           String tableName,
                                                           Object[] columnValues) throws RuntimeException {
    TableDefinition tblDef = getTableDefinition(client, tableName);

    if (tblDef == null || tblDef.getColumnsCount() == 0) {
      System.out.println("Invalid table definition:" + (tblDef == null ? "null" : "OK"));
      throw new RuntimeException("Table definition or Values is null");
    }

    Map<String, String> result = new HashMap<>();
    for (int i = 0; i < columnValues.length; i++) {
      String columnName = tblDef.getColumn(i).getName();
      result.put(columnName.toLowerCase(), columnValues[i].toString());
    }
    return result;
  }

  private static TableDefinition getTableDefinition(DingoClient client, String tableName) {

    /**
     * as the benchmark is on a single table, so the tableDefinition is only one.
     */
    if (tableDefinition == null) {
      tableDefinition = client.getTableDefinition(tableName);
    }
    return tableDefinition;
  }
}
