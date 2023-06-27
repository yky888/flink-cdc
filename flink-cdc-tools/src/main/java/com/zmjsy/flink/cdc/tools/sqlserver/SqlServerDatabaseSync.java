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
package com.zmjsy.flink.cdc.tools.sqlserver;

import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import com.ververica.cdc.connectors.mysql.table.JdbcUrlUtils;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import com.zmjsy.flink.cdc.tools.cdc.DatabaseSync;
import com.zmjsy.flink.cdc.tools.cdc.DateToStringConverter;
import com.zmjsy.flink.cdc.tools.cdc.SourceSchema;
import com.zmjsy.flink.cdc.tools.sqlserver.consts.SqlserverMetadataConsts;
import com.zmjsy.flink.cdc.tools.sqlserver.entity.SqlServerCdcSource;
import com.zmjsy.flink.cdc.tools.sqlserver.function.SqlServerJdbcSourceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class SqlServerDatabaseSync extends DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(SqlServerDatabaseSync.class);

    private static String JDBC_URL = "jdbc:sqlserver://%s:%d;DatabaseName=%s";


    public SqlServerDatabaseSync() {
    }

    @Override
    public Connection getConnection() throws SQLException {
        Properties jdbcProperties = getJdbcProperties();
        StringBuilder jdbcUrlSb = new StringBuilder(JDBC_URL);
        jdbcProperties.forEach((key, value) -> jdbcUrlSb.append("&").append(key).append("=").append(value));
        String jdbcUrl = String.format(jdbcUrlSb.toString(), config.get(SqlServerSourceOptions.HOSTNAME), config.get(SqlServerSourceOptions.PORT),config.get(SqlServerSourceOptions.DATABASE_NAME));

        return DriverManager.getConnection(jdbcUrl,config.get(SqlServerSourceOptions.USERNAME),config.get(SqlServerSourceOptions.PASSWORD));
    }

    @Override
    public List<SourceSchema> getSchemaList() throws Exception {
        String databaseName = config.get(SqlServerSourceOptions.DATABASE_NAME);
        List<SourceSchema> schemaList = new ArrayList<>();
        List<String> tableList = new ArrayList<>();
        SqlServerJdbcSourceFunction sqlServerJdbcSourceFunction = new SqlServerJdbcSourceFunction(config.get(SqlServerSourceOptions.DATABASE_NAME), config.get(SqlServerSourceOptions.HOSTNAME), config.get(SqlServerSourceOptions.PORT), config.get(SqlServerSourceOptions.USERNAME), config.get(SqlServerSourceOptions.PASSWORD));
        Set<SqlServerCdcSource> cdcTableSet =  sqlServerJdbcSourceFunction.getCdcTableSet();
        SqlserverMetadataConsts.SQLSERVER_CDC_SOURCEMAP.put(databaseName,cdcTableSet);
        for(SqlServerCdcSource sqlServerCdcSource:cdcTableSet){
            String sourceTable = sqlServerCdcSource.getSourceTable();
            if (!isSyncNeeded(sourceTable)) {
                continue;
            }
            SourceSchema sourceSchema =
                    new SourceSchema(databaseName,sqlServerCdcSource,"sqlserver");
            schemaList.add(sourceSchema);
            tableList.add(sqlServerCdcSource.getSourceSchema()+"."+sqlServerCdcSource.getSourceTable());
        }

        config.set(SqlServerSourceOptions.TABLE_NAME, String.join(",", tableList));

        // System.out.println(1);

        return schemaList;
    }

    @Override
    public DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env) {
        SqlServerSource.Builder<String> sourceBuilder = SqlServerSource.<String>builder();

        String databaseName = config.get(SqlServerSourceOptions.DATABASE_NAME);
        Preconditions.checkNotNull(databaseName, "database-name in mysql is required");
        String tableName = config.get(SqlServerSourceOptions.TABLE_NAME);
        sourceBuilder
                .hostname(config.get(SqlServerSourceOptions.HOSTNAME))
                .port(config.get(SqlServerSourceOptions.PORT))
                .username(config.get(SqlServerSourceOptions.USERNAME))
                .password(config.get(SqlServerSourceOptions.PASSWORD))
                .database(databaseName)
                .tableList(tableName);
               // .databaseList(databaseName)
            //    .tableList(databaseName + "." + tableName);


        Properties jdbcProperties = new Properties();
        Properties debeziumProperties = new Properties();
        //date to string
        debeziumProperties.putAll(DateToStringConverter.DEFAULT_PROPS);

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
