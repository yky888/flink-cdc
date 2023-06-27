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
package com.zmjsy.flink.cdc.tools.oracle;

import com.ververica.cdc.connectors.mysql.table.JdbcUrlUtils;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import com.zmjsy.flink.cdc.tools.cdc.DatabaseSync;
import com.zmjsy.flink.cdc.tools.cdc.DateToStringConverter;
import com.zmjsy.flink.cdc.tools.cdc.SourceSchema;
import com.zmjsy.flink.cdc.tools.oracle.consts.OracleMetadataConsts;
import com.zmjsy.flink.cdc.tools.oracle.entity.OracleCdcSource;
import com.zmjsy.flink.cdc.tools.oracle.function.OracleJdbcSourceFunction;
import com.zmjsy.flink.cdc.tools.sqlserver.SqlServerSourceOptions;
import com.zmjsy.flink.cdc.tools.sqlserver.consts.SqlserverMetadataConsts;
import com.zmjsy.flink.cdc.tools.sqlserver.entity.SqlServerCdcSource;
import com.zmjsy.flink.cdc.tools.sqlserver.function.SqlServerJdbcSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class OracleDatabaseSync extends DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(OracleDatabaseSync.class);

    private static String JDBC_URL = "jdbc:oracle:thin:@%s:%d/%s";


    public OracleDatabaseSync() {
    }

    @Override
    public Connection getConnection() throws SQLException {
        Properties jdbcProperties = getJdbcProperties();
        StringBuilder jdbcUrlSb = new StringBuilder(JDBC_URL);
        jdbcProperties.forEach((key, value) -> jdbcUrlSb.append("&").append(key).append("=").append(value));
        String jdbcUrl = String.format(jdbcUrlSb.toString(), config.get(OracleSourceOptions.HOSTNAME), config.get(OracleSourceOptions.PORT),config.get(OracleSourceOptions.SERVER_ID));
        config.set(OracleSourceOptions.ORACLE_URL,jdbcUrl);
        return DriverManager.getConnection(jdbcUrl,config.get(OracleSourceOptions.USERNAME),config.get(OracleSourceOptions.PASSWORD));
    }

    @Override
    public List<SourceSchema> getSchemaList() throws Exception {
        String databaseName = config.get(OracleSourceOptions.DATABASE_NAME);
        List<SourceSchema> schemaList = new ArrayList<>();
        List<String> tableList = new ArrayList<>();
        OracleJdbcSourceFunction oracleJdbcSourceFunction = new OracleJdbcSourceFunction(config.get(OracleSourceOptions.DATABASE_NAME),config.get(OracleSourceOptions.SERVER_ID), config.get(OracleSourceOptions.HOSTNAME), config.get(OracleSourceOptions.PORT), config.get(OracleSourceOptions.USERNAME), config.get(OracleSourceOptions.PASSWORD));
        Set<OracleCdcSource> cdcTableSet =  oracleJdbcSourceFunction.getCdcTableSet();
        OracleMetadataConsts.ORACLE_CDC_SOURCEMAP.put(databaseName,cdcTableSet);
        for(OracleCdcSource oracleCdcSource:cdcTableSet){
            String sourceTable = oracleCdcSource.getSourceTable();
            if (!isSyncNeeded(sourceTable)) {
                continue;
            }
            SourceSchema sourceSchema =
                    new SourceSchema(databaseName,oracleCdcSource,"oracle");
            schemaList.add(sourceSchema);
            tableList.add(oracleCdcSource.getSourceSchema()+"."+oracleCdcSource.getSourceTable());
        }

        config.set(OracleSourceOptions.TABLE_NAME, String.join(",", tableList));

        // System.out.println(1);

        return schemaList;
    }

    @Override
    public DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env) {
        OracleSource.Builder<String> sourceBuilder = OracleSource.<String>builder();
        String databaseName = config.get(OracleSourceOptions.DATABASE_NAME);
        Preconditions.checkNotNull(databaseName, "database-name in oracle is required");
        String tableName = config.get(OracleSourceOptions.TABLE_NAME);
        StringBuilder jdbcUrlSb = new StringBuilder(JDBC_URL);
        String jdbcUrl = String.format(jdbcUrlSb.toString(), config.get(OracleSourceOptions.HOSTNAME), config.get(OracleSourceOptions.PORT),config.get(OracleSourceOptions.SERVER_ID));
        config.set(OracleSourceOptions.ORACLE_URL,jdbcUrl);
        sourceBuilder
                .hostname(config.get(OracleSourceOptions.HOSTNAME))
                .port(config.get(OracleSourceOptions.PORT))
                .url(config.get(OracleSourceOptions.ORACLE_URL))
                .username(config.get(OracleSourceOptions.USERNAME))
                .password(config.get(OracleSourceOptions.PASSWORD))
                .database(databaseName)
                .tableList(tableName);
               // .databaseList(databaseName)
            //    .tableList(databaseName + "." + tableName);


        Properties jdbcProperties = new Properties();
        Properties debeziumProperties = new Properties();
        //date to string
        debeziumProperties.putAll(DateToStringConverter.DEFAULT_PROPS);
        debeziumProperties.put("database.dbname",config.get(OracleSourceOptions.DATABASE_NAME));
        debeziumProperties.put("database.pdb.name",config.get(OracleSourceOptions.SERVER_ID));

        for (Map.Entry<String, String> entry : config.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(JdbcUrlUtils.PROPERTIES_PREFIX)) {
                jdbcProperties.put(key.substring(JdbcUrlUtils.PROPERTIES_PREFIX.length()), value);
            } else if (key.startsWith(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX)) {
                debeziumProperties.put(
                        key.substring(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX.length()), value);
            }
        }
        sourceBuilder.debeziumProperties(debeziumProperties);
        DebeziumSourceFunction<String> build = sourceBuilder.deserializer(new JsonDebeziumDeserializationSchema()).build();

        DataStreamSource<String> streamSource = env.addSource(build);
        return streamSource;
    }

    private Properties getJdbcProperties(){
        Properties jdbcProps = new Properties();
        for (Map.Entry<String, String> entry : config.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(JdbcUrlUtils.PROPERTIES_PREFIX)) {
                jdbcProps.put(key.substring(JdbcUrlUtils.PROPERTIES_PREFIX.length()), value);
            }
        }
        return jdbcProps;
    }
}
