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

import com.zmjsy.flink.cdc.tools.doris.DorisType;
import org.apache.flink.util.Preconditions;

public class SqlServerType {
    private static final String BIT = "BIT";
    private static final String BOOLEAN = "BOOLEAN";
    private static final String BOOL = "BOOL";
    private static final String TINYINT = "TINYINT";
    private static final String SMALLINT = "SMALLINT";
    private static final String INT = "INT";
    private static final String INT_IDENTITY = "INT IDENTITY";
    private static final String BIGINT = "BIGINT";
    private static final String REAL = "REAL";
    private static final String FLOAT = "FLOAT";
    private static final String MONEY = "MONEY";
    private static final String SMALL_MONEY = "SMALLMONEY";
    private static final String DECIMAL = "DECIMAL";
    private static final String NUMERIC = "NUMERIC";
    private static final String DATE = "DATE";
    private static final String DATETIME = "DATETIME";
    private static final String DATETIME2 = "DATETIME2";
    private static final String SMALLDATETIME = "SMALLDATETIME";
    private static final String CHAR = "CHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String TEXT = "TEXT";
    private static final String XML = "XML";
    private static final String SQL_VARIANT = "SQL_VARIANT";
    private static final String NCHAR = "NCHAR";
    private static final String NVARCHAR = "NVARCHAR";
    private static final String NTEXT = "NTEXT";
    private static final String BINARY = "BINARY";
    private static final String VARBINARY = "VARBINARY";
    private static final String TIME = "TIME";
    private static final String DATETIMEOFFSET = "datetimeoffset";


    public static String toDorisType(String type) {
        switch (type.toUpperCase()) {
            case BIT:
            case BOOLEAN:
            case BOOL:
                return DorisType.BOOLEAN;
            case TINYINT:
            case SMALLINT:
                return DorisType.SMALLINT;
            case INT:
            case INT_IDENTITY:
                return DorisType.INT;
            case BIGINT:
                return DorisType.BIGINT;
            case REAL:
                return DorisType.FLOAT;
            case FLOAT:
                return DorisType.DOUBLE;
            case MONEY:
            case SMALL_MONEY:
            case DECIMAL:
            case NUMERIC:
                return "DECIMAL(24,6)";
            case DATE:
                return DorisType.DATE;
            case DATETIME:
            case DATETIME2:
            case SMALLDATETIME:
                return DorisType.DATETIME_V2;
            case CHAR:
            case VARCHAR:
            case NCHAR:
            case NVARCHAR:
            case TEXT:
            case NTEXT:
            case BINARY:
            case VARBINARY:
            case TIME:
            case DATETIMEOFFSET:
            case XML:
            case SQL_VARIANT:
                return DorisType.STRING;
            default:
                throw new UnsupportedOperationException("Unsupported sqlserver Type: " + type);
        }

    }
}
