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

import io.dingodb.sdk.client.DingoClient;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

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
  public static final String PRIMARY_KEY = "DINGO_KEY";

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


  public void init() throws DBException {
    Properties props = getProperties();
    String coordinatorList = props.getProperty(COORDINATOR_HOST);

    dingoClient = new DingoClient(coordinatorList);
    boolean isOK = dingoClient.open();
    if (!isOK) {
      throw new DBException("Init connection to coordinator:" + coordinatorList + " failed");
    }
  }

  public void cleanup() throws DBException {
    dingoClient.close();
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }


  @Override
  public Status read(String table,
                     String key,
                     Set<String> fields,
                     Map<String, ByteIterator> result) {
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table,
                       String key,
                       Map<String, ByteIterator> values) {
    return Status.ERROR;
  }

  @Override
  public Status delete(String table,
                       String key) {
    return Status.OK;
  }

  @Override
  public Status update(String table,
                       String key,
                       Map<String, ByteIterator> values) {
    return Status.OK;
  }

  @Override
  public Status scan(String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.OK;
  }
}
