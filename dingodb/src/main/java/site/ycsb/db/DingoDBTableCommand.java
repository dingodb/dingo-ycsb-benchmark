package site.ycsb.db;

import com.alibaba.fastjson.JSONObject;
import io.dingodb.client.DingoClient;
import io.dingodb.common.Common;
import io.dingodb.sdk.common.partition.PartitionDetailDefinition;
import io.dingodb.sdk.common.partition.PartitionRule;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.TableDefinition;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Class to create table in dingoDB cluster to be used by the benchmark.
 *
 */
public final class DingoDBTableCommand {
  public static void main(String[] args) {
    if (args.length == 0) {
      usageMessage();
      System.exit(0);
    }

    String tableName = null;
    int fieldcount = -1;
    Properties props = new Properties();

    // parse arguments
    int argindex = 0;
    while (args[argindex].startsWith("-")) {
      if (args[argindex].compareTo("-c") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        int eq = args[argindex].indexOf('=');
        if (eq < 0) {
          usageMessage();
          System.exit(0);
        }

        String name = args[argindex].substring(0, eq);
        String value = args[argindex].substring(eq + 1);
        props.put(name, value);
        argindex++;
      } else if (args[argindex].compareTo("-p") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        int eq = args[argindex].indexOf('=');
        if (eq < 0) {
          usageMessage();
          System.exit(0);
        }

        String name = args[argindex].substring(0, eq);
        String value = args[argindex].substring(eq + 1);
        props.put(name, value);
        argindex++;
      } else if (args[argindex].compareTo("-n") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        tableName = args[argindex++];
      } else if (args[argindex].compareTo("-f") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        try {
          fieldcount = Integer.parseInt(args[argindex++]);
        } catch (NumberFormatException e) {
          System.err.println("Invalid number for field count");
          usageMessage();
          System.exit(1);
        }
      } else {
        System.out.println("Unknown option " + args[argindex]);
        usageMessage();
        System.exit(0);
      }

      if (argindex >= args.length) {
        break;
      }
    }

    if (argindex != args.length) {
      usageMessage();
      System.exit(0);
    }

    if (tableName == null) {
      System.err.println("table name missing.");
      usageMessage();
      System.exit(1);
    }

    if (fieldcount > 0) {
      props.setProperty(DingoDBClient.FIELD_COUNT_PROPERTY, String.valueOf(fieldcount));
    }

    try {
      doCommandOnTable(props, tableName);
    } catch (Exception e) {
      System.err.println("Error in creating table. " + e);
      System.exit(1);
    }
  }

  private static void usageMessage() {
    System.out.println("Do Table Command(create/drop) Client. Options:");
    System.out.println("  -c   command=operation(default create, such as:"
            + DingoDBClient.DINGO_TBL_COMMAND+ "=create/drop)");
    System.out.println("  -p   key=value properties defined("
        + DingoDBClient.COORDINATOR_HOST + "=172.20.3.13:22001)");
    System.out.println("  -n   name of the table.");
    System.out.println("  -f   number of fields (default 10).");
  }

  private static void doCommandOnTable(Properties props, String tableName) throws Exception {
    String coordinatorList = props.getProperty(DingoDBClient.COORDINATOR_HOST);
    int columnCnt = Integer.parseInt(
        props.getProperty(
            DingoDBClient.FIELD_COUNT_PROPERTY,
            DingoDBClient.FIELD_COUNT_PROPERTY_DEFAULT
        )
    );

    String tableCommand = props.getProperty(
        DingoDBClient.DINGO_TBL_COMMAND,
        DingoDBClient.DINGO_TBL_COMMAND_DEFAULT);

    if (coordinatorList == null || coordinatorList.isEmpty()) {
      throw new Exception("Missing connection information.");
    }

    DingoClient dingoClient = new DingoClient(coordinatorList, 10);
    
    boolean isOK = dingoClient.open();
    if (!isOK) {
      throw new Exception("Create Connection to Coordinator[" + coordinatorList + "] failed");
    }

    boolean isDropTable = (0 == tableCommand.compareToIgnoreCase(DingoDBClient.DINGO_TBL_COMMAND_DROP));
    if (isDropTable) {
      dingoClient.dropTable(tableName);
      return;
    }
//    TableDefinition tableDef = DingoDBClient.getTableDefinition(tableName);
    List<Column> colDefList = new ArrayList<>();
    final String defaultTypeName = "varchar";
    ColumnDefinition primaryColumn = new ColumnDefinition(
        DingoDBClient.PRIMARY_KEY,
        defaultTypeName,
        "",
        -1,
        1,
        false,
        0,
        generateRandomStr(20),
        false
    );
    colDefList.add(primaryColumn);

    for (int i = 0; i < columnCnt; i++) {
      ColumnDefinition colDef = new ColumnDefinition(
          DingoDBClient.COLUMN_PREFIX + i,
          defaultTypeName,
          "",
          -1,
          1,
          true,
          -1,
          generateRandomStr(20),
          false
      );
      colDefList.add(colDef);
    }

    PartitionDetailDefinition partitionDetailDefinition = new PartitionDetailDefinition(
        null, null, Arrays.asList(new Object[]{"a"}));
    PartitionRule partitionRule = new PartitionRule(
        null, null, Arrays.asList(partitionDetailDefinition));

//    TableDefinition tableDef = new TableDefinition(
//        tableName,
//        colDefList,
//        1,
//        0,
//        null,
//        Common.Engine.ENG_ROCKSDB.name(),
//        null
//    );
    
    TableDefinition tableDef = TableDefinition.builder()
        .name(tableName)
        .columns(colDefList)
        .version(1)
        .ttl(0)
        .partition(null)
        .engine(Common.Engine.ENG_ROCKSDB.name())
        .replica(3)
        .createSql("")
        .build();
    
    System.out.println("=========================================================");
    System.out.println(toJson(tableDef));
    System.out.println("=========================================================");

    boolean createStatus = dingoClient.createTable(tableDef);
    if (!createStatus) {
      System.out.println("create table failed!");
    } 
    dingoClient.close();
  }

  //definition转json
  public static String toJson(TableDefinition tableDefinition) {
    Map<String, Object> defMap = new LinkedHashMap<>();
    List<Column> cl = tableDefinition.getColumns();
    for (Column column : cl) {
      defMap.put(column.getName(), column.getType());
    }
    JSONObject jsonObject = new JSONObject(defMap);
    return jsonObject.toJSONString();
  }

  private static String generateRandomStr(int strLen) {
    return RandomStringUtils.randomAlphanumeric(strLen);
  }

//  private static String generateRandomString(int expectedLen) {
//    // letter 'a'
//    int leftLimit = 97;
//    // letter 'z'
//    int rightLimit = 122;
//    Random random = new Random();
//
//    String generatedString = random
//        .ints(leftLimit, rightLimit + 1)
//        .limit(expectedLen)
//        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
//        .toString();
//    return generatedString;
//  }

  private DingoDBTableCommand() {
    super();
  }
}
