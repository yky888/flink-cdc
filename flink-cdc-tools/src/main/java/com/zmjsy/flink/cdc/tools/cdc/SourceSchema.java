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
package com.zmjsy.flink.cdc.tools.cdc;


import com.zmjsy.flink.cdc.tools.doris.DataModel;
import com.zmjsy.flink.cdc.tools.doris.FieldSchema;
import com.zmjsy.flink.cdc.tools.doris.TableSchema;
import com.zmjsy.flink.cdc.tools.mysql.MysqlType;
import com.zmjsy.flink.cdc.tools.oracle.OracleType;
import com.zmjsy.flink.cdc.tools.oracle.entity.OracleCdcSource;
import com.zmjsy.flink.cdc.tools.sqlserver.SqlServerType;
import com.zmjsy.flink.cdc.tools.sqlserver.entity.SqlServerCdcSource;
import com.zmjsy.flink.cdc.tools.sqlserver.entity.TableDesc;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SourceSchema implements Serializable {
    private  String databaseName;
    private  String tableName;
    private  String tableComment;
    private  LinkedHashMap<String, FieldSchema> fields;
    public  List<String> primaryKeys;
    private  String databaseType;

    public SourceSchema(
            DatabaseMetaData metaData, String databaseName, String tableName, String tableComment,String databaseType)
            throws Exception {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableComment = tableComment;
        this.databaseType = databaseType;

        fields = new LinkedHashMap<>();
        try (ResultSet rs = metaData.getColumns(databaseName, null, tableName, null)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                String comment = rs.getString("REMARKS");
                String fieldType = rs.getString("TYPE_NAME");
                Integer precision = rs.getInt("COLUMN_SIZE");

                if (rs.wasNull()) {
                    precision = null;
                }
                Integer scale = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    scale = null;
                }
                String dorisTypeStr = "STRING";
                switch (databaseType.toUpperCase()){
                    case "MYSQL":
                        dorisTypeStr =  MysqlType.toDorisType(fieldType, precision, scale);
                        break;
                    case "SQLSERVER":
                        dorisTypeStr =  SqlServerType.toDorisType(fieldType);
                }
                fields.put(fieldName, new FieldSchema(fieldName, dorisTypeStr, comment));
            }
        }

        primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, null, tableName)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                primaryKeys.add(fieldName);
            }
        }
    }

    // sqlserver 构建
    public SourceSchema(String databaseName,SqlServerCdcSource sqlServerCdcSource, String databaseType) {
        this.databaseName=databaseName;
        this.tableName=sqlServerCdcSource.getSourceTable();
        this.tableComment=sqlServerCdcSource.getTableDesc();
        this.databaseType=databaseType;
        fields = new LinkedHashMap<>();
        primaryKeys = new ArrayList<>();
        List<TableDesc> ddl = sqlServerCdcSource.getDdl();
        for(TableDesc tableDesc:ddl){
            String fieldName = tableDesc.getColumnName();
            String comment = tableDesc.getColumnComment();
            String columnType = tableDesc.getColumnType();
            String isUniqueKey = tableDesc.getIsUniqueKey();
            if("1".equals(isUniqueKey)){
                primaryKeys.add(fieldName);
            }
            String dorisTypeStr = SqlServerType.toDorisType(columnType);
            fields.put(fieldName, new FieldSchema(fieldName, dorisTypeStr, comment));
        }

    }

    // sqlserver 构建
    public SourceSchema(String databaseName, OracleCdcSource oracleCdcSource, String databaseType) {
        this.databaseName=databaseName;
        this.tableName=oracleCdcSource.getSourceTable();
        this.tableComment=oracleCdcSource.getTableDesc();
        this.databaseType=databaseType;
        fields = new LinkedHashMap<>();
        primaryKeys = new ArrayList<>();
        List<TableDesc> ddl = oracleCdcSource.getDdl();
        for(TableDesc tableDesc:ddl){
            String fieldName = tableDesc.getColumnName();
            String comment = tableDesc.getColumnComment();
            String columnType = tableDesc.getColumnType();
            String isUniqueKey = tableDesc.getIsUniqueKey();
            if("1".equals(isUniqueKey)){
                primaryKeys.add(fieldName);
            }
            String dorisTypeStr = OracleType.toDorisType(columnType);
            fields.put(fieldName, new FieldSchema(fieldName, dorisTypeStr, comment));
        }

    }


    public TableSchema convertTableSchema(Map<String, String> tableProps) {
        TableSchema tableSchema = new TableSchema();
        tableSchema.setModel(DataModel.UNIQUE);
        tableSchema.setFields(this.fields);
        tableSchema.setKeys(this.primaryKeys);
        tableSchema.setTableComment(this.tableComment);
        tableSchema.setDistributeKeys(this.primaryKeys);
        tableSchema.setProperties(tableProps);
        return tableSchema;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public LinkedHashMap<String, FieldSchema> getFields() {
        return fields;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public String getTableComment() {
        return tableComment;
    }


}
