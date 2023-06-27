package com.zmjsy.flink.cdc.tools.sqlserver.consts;

import java.util.HashMap;
import java.util.Map;

/**
 * @BelongsProject: data-platform-parent
 * @BelongsPackage: com.zmj.flink.cdc.consts
 * @Author: kaiyuanyang
 * @CreateTime: 2023-06-09  15:22
 * @Description: TODO
 * @Version: 1.0
 */
public class DorisMetadataConsts {

    public static final String CREATE_DDL = "CREATE TABLE IF NOT EXISTS ${table_name} (\n" +
             "${column_list}"+
            ") ENGINE=OLAP\n" +
            "UNIQUE KEY(${pk})\n" +
            "COMMENT '${tableDesc}'\n" +
            "DISTRIBUTED BY HASH(${pk}) BUCKETS 4\n" +
            "PROPERTIES (\n" +
            "\"replication_allocation\" = \"tag.location.default: 3\",\n" +
            "\"in_memory\" = \"false\",\n" +
            "\"storage_format\" = \"V2\",\n" +
            "\"disable_auto_compaction\" = \"false\"\n" +
            ");";

    public static final Map<String,String> SQLSERVER_DORIS_MAPPING = new HashMap<String, String>(){
        {
                    put("bit","boolean");
                    put("tinyint","smallint");
                    put("smallint","smallint");
                    put("int","int");
                    put("bigint","bigint");
                    put("real","float");
                    put("float","double");
                    put("money","decimal(19,4)");
                    put("smallmoney","decimal(10,4)");
                    put("decimal","decimal(24,6)");
                    put("numeric","decimal(24,6)");
                    put("date","date");
                    put("datetime","datetimev2");
                    put("datetime2","datetimev2");
                    put("smalldatetime","datetimev2");
                    put("char","string");
                    put("varchar","string");
                    put("text","string");
                    put("nchar","string");
                    put("nvarchar","string");
                    put("ntext","string");
                    put("binary","string");
                    put("varbinary","string");
                    put("time","string");
                    put("xml","string");
                    put("sql_variant","string");
                    put("datetimeoffset","string");
        }
    };

    public static final String DATE_TYPE_LIST="smalldatetime,datetime,datetime2,timestamp(6),timestamp(3),timestamp(0)";




}
