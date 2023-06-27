// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package com.zmjsy.flink.cdc.tools;


import com.zmjsy.flink.cdc.tools.cdc.DatabaseSync;
import com.zmjsy.flink.cdc.tools.mysql.MysqlDatabaseSync;
import com.zmjsy.flink.cdc.tools.oracle.OracleDatabaseSync;
import com.zmjsy.flink.cdc.tools.sqlserver.SqlServerDatabaseSync;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * cdc sync tools
 */
//@Slf4j
public class CdcTools {
    private static final String MYSQL_SYNC_DATABASE = "mysql-sync-database";
    private static final String SQLSERVER_SYNC_DATABASE = "sqlserver-sync-database";
    private static final String ORACLE_SYNC_DATABASE = "oracle-sync-database";
    private static final List<String> EMPTY_KEYS = Arrays.asList("password");

    public static void main(String[] args) throws Exception {



//        args=new String[]{"mysql-sync-database",
//                "--database", "xxxxx",
//                "--mysql-conf", "hostname=xxxxxx",
//                "--mysql-conf", "username=xxxx",
//                "--mysql-conf", "password=xxxx",
//                "--mysql-conf", "database-name=xxxx",
//                "--including-tables","..*",
//                "--sink-conf", "fenodes=xxxx:8030,xxxx:8030,xxxx:8030",
//                "--sink-conf", "username=xxx",
//                "--sink-conf", "password=xxxx",
//                "--sink-conf", "jdbc-url=jdbc:mysql://xxxx:9030",
//                "--sink-conf", "sink.label-prefix=label",
//                "--table-conf", "replication_num=3"
//        };


//        args=new String[]{"sqlserver-sync-database",
//                "--database", "xxxxx",
//                "--sqlserver-conf", "hostname=xxxx",
//                "--sqlserver-conf", "username=xxx",
//                "--sqlserver-conf", "password=xxxx",
//                "--sqlserver-conf", "database-name=xxx",
//                "--including-tables","..*",
//                "--sink-conf", "fenodes=xxxx:8030,xxxx:8030,xxxx:8030",
//                "--sink-conf", "username=xxx",
//                "--sink-conf", "password=xxxx",
//                "--sink-conf", "jdbc-url=jdbc:mysql://xxxx:9030",
//                "--sink-conf", "sink.label-prefix=label1",
//                "--sink-conf", "sink.utc=8",
//                "--table-conf", "replication_num=3"
//        };


//        args=new String[]{"oracle-sync-database",
//                "--database", "xxxx",
//                "--oracle-conf", "hostname=xxxx",
//                "--oracle-conf", "username=xxxx",
//                "--oracle-conf", "password=xxxx",
//                "--oracle-conf", "database-name=xxxx",
//                "--oracle-conf", "server-id=xxxx",
//                "--including-tables","..*",
//                "--sink-conf", "fenodes=xxxx:8030,xxxx:8030,xxxx:8030",
//                "--sink-conf", "username=xxx",
//                "--sink-conf", "password=xxx",
//                "--sink-conf", "jdbc-url=jdbc:mysql://xxxx:9030",
//                "--sink-conf", "sink.label-prefix=label3",
//                "--sink-conf", "sink.utc=8",
//                "--table-conf", "replication_num=3"
//        };


        System.out.println("args:======"+Arrays.asList(args));
        String operation = args[0].toLowerCase();
        System.out.println("operation:======"+operation);
        String[] opArgs = Arrays.copyOfRange(args, 1, args.length);
        System.out.println();
        switch (operation) {
            case MYSQL_SYNC_DATABASE:
                createMySQLSyncDatabase(opArgs);
                break;
            case SQLSERVER_SYNC_DATABASE:
                createSqlServerSyncDatabase(opArgs);
                break;
            case ORACLE_SYNC_DATABASE:
                createOracleSyncDatabase(opArgs);
            default:
                System.out.println("Unknown operation " + operation);
                System.exit(1);
        }
    }



    private static void createOracleSyncDatabase(String[] opArgs) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        String jobName = params.get("job-name");
        String database = params.get("database");
        String tablePrefix = params.get("table-prefix");
        String tableSuffix = params.get("table-suffix");
        String includingTables = params.get("including-tables");
        String excludingTables = params.get("excluding-tables");

        Map<String, String> oracleMap = getConfigMap(params, "oracle-conf");
        Map<String, String> sinkMap = getConfigMap(params, "sink-conf");
        Map<String, String> tableMap = getConfigMap(params, "table-conf");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);
        Configuration oracleConfig = Configuration.fromMap(oracleMap);
        Configuration sinkConfig = Configuration.fromMap(sinkMap);

        DatabaseSync databaseSync = new OracleDatabaseSync();
        databaseSync.create(env, database, oracleConfig, tablePrefix, tableSuffix, includingTables, excludingTables, sinkConfig, tableMap);
        databaseSync.build();

        if(StringUtils.isNullOrWhitespaceOnly(jobName)){
            jobName = String.format("Doris Sync Database: %s", oracleMap.get("database-name"));
        }
        env.execute(jobName);
    }




    private static void createSqlServerSyncDatabase(String[] opArgs) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        String jobName = params.get("job-name");
        String database = params.get("database");
        String tablePrefix = params.get("table-prefix");
        String tableSuffix = params.get("table-suffix");
        String includingTables = params.get("including-tables");
        String excludingTables = params.get("excluding-tables");

        Map<String, String> sqlserverMap = getConfigMap(params, "sqlserver-conf");
        Map<String, String> sinkMap = getConfigMap(params, "sink-conf");
        Map<String, String> tableMap = getConfigMap(params, "table-conf");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);
        Configuration sqlserverConfig = Configuration.fromMap(sqlserverMap);
        Configuration sinkConfig = Configuration.fromMap(sinkMap);

        DatabaseSync databaseSync = new SqlServerDatabaseSync();
        databaseSync.create(env, database, sqlserverConfig, tablePrefix, tableSuffix, includingTables, excludingTables, sinkConfig, tableMap);
        databaseSync.build();

        if(StringUtils.isNullOrWhitespaceOnly(jobName)){
            jobName = String.format("Doris Sync Database: %s", sqlserverMap.get("database-name"));
        }
        env.execute(jobName);
    }




    private static void createMySQLSyncDatabase(String[] opArgs) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        String jobName = params.get("job-name");
        String database = params.get("database");
        String tablePrefix = params.get("table-prefix");
        String tableSuffix = params.get("table-suffix");
        String includingTables = params.get("including-tables");
        String excludingTables = params.get("excluding-tables");

        Map<String, String> mysqlMap = getConfigMap(params, "mysql-conf");
        Map<String, String> sinkMap = getConfigMap(params, "sink-conf");
        Map<String, String> tableMap = getConfigMap(params, "table-conf");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration mysqlConfig = Configuration.fromMap(mysqlMap);
        Configuration sinkConfig = Configuration.fromMap(sinkMap);

        DatabaseSync databaseSync = new MysqlDatabaseSync();
        databaseSync.create(env, database, mysqlConfig, tablePrefix, tableSuffix, includingTables, excludingTables, sinkConfig, tableMap);
        databaseSync.build();

        if(StringUtils.isNullOrWhitespaceOnly(jobName)){
            jobName = String.format("MySQL-Doris Sync Database: %s", mysqlMap.get("database-name"));
        }
        env.execute(jobName);
    }

    private static Map<String, String> getConfigMap(MultipleParameterTool params, String key) {
        if (!params.has(key)) {
            return null;
        }

        Map<String, String> map = new HashMap<>();
        for (String param : params.getMultiParameter(key)) {
            String[] kv = param.split("=");
            if (kv.length == 2) {
                map.put(kv[0], kv[1]);
                continue;
            }else if(kv.length == 1 && EMPTY_KEYS.contains(kv[0])){
                map.put(kv[0], "");
                continue;
            }

            System.err.println(
                    "Invalid " + key + " " + param + ".\n");
            return null;
        }
        return map;
    }
}