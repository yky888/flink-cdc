package com.zmjsy.flink.cdc.tools.oracle.consts;

import com.zmjsy.flink.cdc.tools.oracle.entity.OracleCdcSource;
import com.zmjsy.flink.cdc.tools.sqlserver.entity.SqlServerCdcSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @BelongsProject: data-platform-parent
 * @BelongsPackage: com.zmj.flink.cdc.consts
 * @Author: kaiyuanyang
 * @CreateTime: 2023-06-09  14:34
 * @Description: TODO
 * @Version: 1.0
 */
public class OracleMetadataConsts {


    //查询有主键的表列表
    public static final String META_PK_TABLE_LIST_SQL = "select\n" +
            "\tt1.table_name,\n" +
            "\tt1.comments\n" +
            "from\n" +
            "\tall_tab_comments t1\n" +
            "inner join dba_constraints t2 on t1.table_name = t2.table_name and t2.constraint_type = 'P' and t1.owner = t2.owner \n" +
            "where\n" +
            "\tt1.owner = upper('${databaseName}')";



    // 查询 表字段 源数据信息
    public static final String META_SCHEMA_SQL="SELECT\n" +
            "\tt1.TABLE_NAME AS table_name,\n" +
            "\t'' AS table_comment,\n" +
            "\tt1.COLUMN_NAME AS column_name,\n" +
            "\tt1.DATA_TYPE  AS column_type,\n" +
            "\tt3.COMMENTS AS column_comment,\n" +
            "\tCASE WHEN t2.COLUMN_NAME IS NULL THEN '0' ELSE '1' END AS is_null,\n" +
            "\tCASE WHEN t2.COLUMN_NAME IS NULL THEN '0' ELSE '1' END AS is_unique_key\n" +
            "FROM\n" +
            "\tall_tab_columns t1\n" +
            "LEFT JOIN(\n" +
            "\tSELECT\n" +
            "\t\tt2.TABLE_OWNER,t2.TABLE_NAME,t2.COLUMN_NAME\n" +
            "\tFROM\n" +
            "\t\tall_indexes t1\n" +
            "\tINNER JOIN all_ind_columns t2 ON t1.OWNER = t2.INDEX_OWNER AND t1.TABLE_NAME = t2.TABLE_NAME AND t1.INDEX_NAME = t2.index_name\n" +
            "\tWHERE\n" +
            "\t\tt1.UNIQUENESS = 'UNIQUE'\n" +
            "\tGROUP BY t2.TABLE_OWNER,t2.TABLE_NAME,t2.COLUMN_NAME\t\n" +
            ") t2 ON t1.OWNER = t2.TABLE_OWNER AND t1.TABLE_NAME = t2.TABLE_NAME AND t1.COLUMN_NAME  = t2.COLUMN_NAME\n" +
            "LEFT JOIN all_col_comments t3 ON t1.OWNER = t3.OWNER AND t1.TABLE_NAME = t3.TABLE_NAME AND t1.COLUMN_NAME = t3.COLUMN_NAME \n" +
            "WHERE \n" +
            "\t t1.OWNER = upper('${databaseName}')\n" +
            "ORDER BY t1.TABLE_NAME ,t1.COLUMN_ID";

    // 开启全字段日志
    public static final String ORACLE_LOG_DATA_ALTER_SQL = "ALTER TABLE ${databaseName}.${tableName} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS";

    //查询开启全字段日志表列表

    public static final String ORACLE_LOG_DATA_TABLE_LIST_SQL = "SELECT TABLE_NAME  FROM DBA_LOG_GROUPS WHERE OWNER = '${databaseName}' AND LOG_GROUP_TYPE = 'ALL COLUMN LOGGING'";

    public static Map<String, Set<OracleCdcSource>> ORACLE_CDC_SOURCEMAP = new HashMap<>();



}
