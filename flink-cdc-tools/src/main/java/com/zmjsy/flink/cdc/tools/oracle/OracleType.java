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

import com.zmjsy.flink.cdc.tools.doris.DorisType;

public class OracleType {
    private static final String NUMBER = "NUMBER";
    private static final String DECIMAL = "DECIMAL";
    private static final String FLOAT = "FLOAT";
    private static final String REAL = "REAL";
    private static final String DATE = "DATE";
    private static final String TIMESTAMP6 = "TIMESTAMP(6)";
    private static final String TIMESTAMP0 = "TIMESTAMP(0)";
    private static final String TIMESTAMP3 = "TIMESTAMP(3)";
    private static final String CHAR = "CHAR";
    private static final String NCHAR = "NCHAR";
    private static final String VARCHAR2 = "VARCHAR2";
    private static final String NVARCHAR2 = "NVARCHAR2";
    private static final String LONG = "LONG";
    private static final String RAW = "RAW";
    private static final String LONG_RAW = "LONG RAW";
    private static final String INTERVAL = "INTERVAL";
    private static final String CLOB = "CLOB";



    public static String toDorisType(String type) {
        switch (type.toUpperCase()) {
            case NUMBER:
            case DECIMAL:
                return "DECIMAL(24,6)";
            case FLOAT:
            case REAL:
                return DorisType.DOUBLE;
            case DATE:
            case TIMESTAMP6:
            case TIMESTAMP0:
            case TIMESTAMP3:
                return DorisType.DATETIME;
            case CHAR:
            case NCHAR:
            case VARCHAR2:
            case NVARCHAR2:
            case LONG:
            case RAW:
            case LONG_RAW:
            case INTERVAL:
            case CLOB:
                return DorisType.STRING;
            default:
                throw new UnsupportedOperationException("Unsupported oracle Type: " + type);
        }

    }
}
