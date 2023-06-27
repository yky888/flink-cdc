package com.zmj.flink.cdc.enums;

public enum DorisSqlserverFieldMapping {

    ENUM_BIT("bit","boolean"),
    ENUM_THINYINT("tinyint","smallint"),
    ENUM_SMALLINT("smallint","smallint"),
    ENUM_INT("int","int"),
    ENUM_REAL("real","float"),
    ENUM_FLOAT("float","double"),
    ENUM_MONEY("money","decimal(19,4)"),
    ENUM_SMALLMONEY("smallmoney","decimal(10,4)"),
    ENUM_DECIMAL("decimal","decimal(24,6)"),
    ENUM_NUMERIC("numeric","decimal(24,6)"),
    ENUM_DATE("date","date"),
    ENUM_DATETIME("datetime","datetimev2"),
    ENUM_DATETIME2("datetime2","datetimev2"),
    ENUM_SMALLDATETIME("smalldatetime","datetimev2"),
    ENUM_CHAR("char","string"),
    ENUM_VARCHAR("varchar","string"),
    ENUM_TEXT("text","string"),
    ENUM_NCHAR("nchar","string"),
    ENUM_NVARCHAR("nvarchar","string"),
    ENUM_NTEXT("ntext","string"),
    ENUM_BINARY("binary","string"),
    ENUM_VARBINARY("varbinary","string"),
    ENUM_TIME("time","STRING"),
    ENUM_DATETIMEOFFSET("datetimeoffset","string")
    ;

    private String sqlserverType;

    private String dorisType;

    DorisSqlserverFieldMapping(String sqlserverType, String dorisType) {
        this.sqlserverType = sqlserverType;
        this.dorisType = dorisType;
    }
}
