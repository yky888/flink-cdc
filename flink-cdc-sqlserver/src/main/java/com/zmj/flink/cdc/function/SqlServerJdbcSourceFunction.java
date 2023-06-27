package com.zmj.flink.cdc.function;

import com.zmj.flink.cdc.consts.DorisMetadataConsts;
import com.zmj.flink.cdc.consts.SqlserverMetadataConsts;
import com.zmj.flink.cdc.entity.SqlServerCdcSource;
import com.zmj.flink.cdc.entity.TableDesc;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static com.zmj.flink.cdc.consts.DorisMetadataConsts.SQLSERVER_DORIS_MAPPING;

/**
 * @BelongsProject: data-platform-parent
 * @BelongsPackage: com.zmj.flink.cdc.utils
 * @Author: kaiyuanyang
 * @CreateTime: 2023-06-09  10:13
 * @Description: sqlserver 数据源获取
 * @Version: 1.0
 */
@Slf4j
public class SqlServerJdbcSourceFunction {

    private String databaseName;

    private String hostName;

    private int port = 1433;

    private String user;

    private String password;

    private  String url = "jdbc:sqlserver://${hostName}:${port};DatabaseName=${databaseName};";

    private  Connection connection;

    private Statement statement;


    public SqlServerJdbcSourceFunction() {
    }

    public SqlServerJdbcSourceFunction(String databaseName, String hostName, int port, String user, String password) {
        this.databaseName = databaseName;
        this.hostName = hostName;
        this.port = port;
        this.user = user;
        this.password = password;
        this.url=this.url.replace("${hostName}",hostName).replace("${port}",String.valueOf(port)).replace("${databaseName}",databaseName);
       try {
           connection = getConnection();
       }catch (Exception e){
           log.error("获取连接异常:{}",e);
       }
    }

    public  Connection getConnection() throws Exception{
        Connection conn = DriverManager.getConnection(url, user, password);
        DatabaseMetaData metaData = conn.getMetaData();
        return conn;
    }

    public Set<SqlServerCdcSource> getCdcTableSet() throws Exception{
        statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("exec sys.sp_cdc_help_change_data_capture");
        Set<SqlServerCdcSource> sqlServerCdcSourceSet = new HashSet<>();
        while (rs.next()){
            String index_name = rs.getString("index_name");
            if("null".equals(index_name)||StringUtils.isBlank(index_name)){
                continue;
            }
            SqlServerCdcSource sqlServerCdcSource = SqlServerCdcSource.builder()
                    .sourceSchema(rs.getString("source_schema").toLowerCase())
                    .sourceTable(rs.getString("source_table").toLowerCase())
                    .objectId(rs.getInt("object_id"))
                    .sourceObjectId(rs.getInt("source_object_id"))
                    .indexColumnList(rs.getString("index_column_list"))
                    .capturedColumnList(rs.getString("captured_column_list"))
                    .build();
            sqlServerCdcSourceSet.add(sqlServerCdcSource);
        }
        // 获取字段信息
        ResultSet rs2 = statement.executeQuery(SqlserverMetadataConsts.META_SCHEMA_SqL);
        List<TableDesc> tableDescList = new ArrayList<>();
        while (rs2.next()){
            TableDesc tableDesc = TableDesc.builder()
                    .tableName(rs2.getString("table_name").toLowerCase())
                    .columnName(rs2.getString("column_name").toLowerCase())
                    .columnType(rs2.getString("column_type").toLowerCase())
                    .columnComment(rs2.getString("column_comment"))
                    .isNull(rs2.getString("is_null"))
                    .isUniqueKey(rs2.getString("is_pk"))
                    .build();
            tableDescList.add(tableDesc);
        }
        Map<String, List<TableDesc>> collect = tableDescList.stream().collect(Collectors.groupingBy(TableDesc::getTableName));
        sqlServerCdcSourceSet.stream().map(cdc->{
            List<TableDesc> list = collect.get(cdc.getSourceTable().toLowerCase());
            cdc.setDdl(list);
            String pk = cdc.getIndexColumnList().replace("[", "").replace("]", "").trim().toLowerCase();
            StringBuilder sbpk = new StringBuilder();
            StringBuilder sb = new StringBuilder();

            //首先添加 pk
            String[] pks = pk.split(",");
            for(String p:pks){
                sbpk.append(" "+p);
                sbpk.append(" ${"+p+"}");
                sbpk.append(" not null");
                sbpk.append(" comment '主键'");
                sbpk.append(",\n");
            }

            String pkstr = sbpk.toString();

            if(list!=null&&list.size()>0){
                int i =0;
                for(TableDesc tableDesc:list){
                    i++;
                    if(pk.contains(tableDesc.getColumnName())){
                        pkstr=pkstr.replace("${"+tableDesc.getColumnName()+"}",tableDesc.getColumnType());
                        continue;
                    }
                    sb.append(" "+tableDesc.getColumnName());
                    sb.append(" "+ SQLSERVER_DORIS_MAPPING.get(tableDesc.getColumnType()));
                    sb.append(" "+ tableDesc.getIsNull());
                    String tmp = ("null".equals(tableDesc.getColumnComment())|| StringUtils.isBlank(tableDesc.getColumnComment()))==true?"":tableDesc.getColumnComment();
                    sb.append(" comment '" +tmp+"'");
                    if(i==list.size()){
                        sb.append("\n");
                    }else{
                        sb.append(",\n");
                    }
                }
            }
            sb.insert(0,pkstr);
            String dorisddl = DorisMetadataConsts.CREATE_DDL.replace("${table_name}", cdc.getSourceTable()).replace("${pk}", pk).replace("${column_list}", sb.toString());
            cdc.setDorisDdl(dorisddl);
            return cdc;
        }).collect(Collectors.toList());

        statement.close();
        connection.close();
        return sqlServerCdcSourceSet;
    }
//
//    public List<SourceSchema> getSchemaList() throws Exception {
//        String databaseName = config.get(SqlServerSourceOptions.DATABASE_NAME);
//        List<SourceSchema> schemaList = new ArrayList<>();
//        try (Connection conn = getConnection()) {
//            DatabaseMetaData metaData = conn.getMetaData();
//            try (ResultSet tables =
//                         metaData.getTables(databaseName, null, "%", new String[]{"TABLE"})) {
//                while (tables.next()) {
//                    String tableName = tables.getString("TABLE_NAME");
//                    String tableComment = tables.getString("REMARKS");
//                    if (!isSyncNeeded(tableName)) {
//                        continue;
//                    }
//                    SourceSchema sourceSchema =
//                            new SourceSchema(metaData, databaseName, tableName, tableComment);
//                    if (sourceSchema.primaryKeys.size() > 0) {
//                        //Only sync tables with primary keys
//                        schemaList.add(sourceSchema);
//                    } else {
//                        LOG.warn("table {} has no primary key, skip", tableName);
//                        System.out.println("table " + tableName + " has no primary key, skip.");
//                    }
//                }
//            }
//        }
//        return schemaList;
//    }






}
