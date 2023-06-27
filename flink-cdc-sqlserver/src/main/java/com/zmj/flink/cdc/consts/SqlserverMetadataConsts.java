package com.zmj.flink.cdc.consts;

/**
 * @BelongsProject: data-platform-parent
 * @BelongsPackage: com.zmj.flink.cdc.consts
 * @Author: kaiyuanyang
 * @CreateTime: 2023-06-09  14:34
 * @Description: TODO
 * @Version: 1.0
 */
public class SqlserverMetadataConsts {

    // 查询 表字段 源数据信息
    public static final String META_SCHEMA_SqL="SELECT\n" +
            "\ta.name AS table_name,\n" +
            "\tb.name AS column_name,\n" +
            "\tc.name AS column_type,\n" +
            "\tCASE\n" +
            "\t\tWHEN b.is_nullable = 0 THEN\n" +
            "            'not null'\n" +
            "\t\tELSE\n" +
            "            'null'\n" +
            "\tEND AS is_null,\n" +
            "\td.value AS column_comment,\n" +
            "\tcase \n" +
            "\t\twhen e.COLUMN_NAME is not null then '1'\n" +
            "\t\telse '0'\n" +
            "\tend as is_pk\n" +
            "from\n" +
            "\tsys.tables a\n" +
            "inner join sys.columns b on\n" +
            "\tb.object_id = a.object_id\n" +
            "inner join sys.types c on\n" +
            "\tb.system_type_id = c.system_type_id\n" +
            "left join sys.extended_properties d on\n" +
            "\ta.object_id = d.major_id\n" +
            "\tand d.minor_id = b.column_id\n" +
            "left join information_schema.key_column_usage e on \n" +
            "\ta.name =  e.TABLE_NAME and b.name = e.COLUMN_NAME \n" +
            "where \n" +
            "\tc.name <> 'sysname' and a.schema_id = 1\n" +
            "order by a.object_id ,b.column_id ";



}
