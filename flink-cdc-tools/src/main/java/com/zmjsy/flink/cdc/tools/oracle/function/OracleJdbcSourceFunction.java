package com.zmjsy.flink.cdc.tools.oracle.function;

import com.zmjsy.flink.cdc.tools.oracle.consts.OracleMetadataConsts;
import com.zmjsy.flink.cdc.tools.oracle.entity.OracleCdcSource;
import com.zmjsy.flink.cdc.tools.sqlserver.consts.DorisMetadataConsts;
import com.zmjsy.flink.cdc.tools.sqlserver.consts.SqlserverMetadataConsts;
import com.zmjsy.flink.cdc.tools.sqlserver.entity.SqlServerCdcSource;
import com.zmjsy.flink.cdc.tools.sqlserver.entity.TableDesc;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;


/**
 * @BelongsProject: data-platform-parent
 * @BelongsPackage: com.zmj.flink.cdc.utils
 * @Author: kaiyuanyang
 * @CreateTime: 2023-06-09  10:13
 * @Description: sqlserver 数据源获取
 * @Version: 1.0
 */
@Slf4j
public class OracleJdbcSourceFunction {

    private String databaseName;

    private String hostName;

    private int port = 1521;

    private String user;

    private String password;

    private  String url = "jdbc:oracle:thin:@${hostName}:${port}/${serviceName}";

    private  Connection connection;

    private Statement statement;


    public OracleJdbcSourceFunction() {
    }

    public OracleJdbcSourceFunction(String databaseName, String serviceName,String hostName, int port, String user, String password) {
        this.databaseName = databaseName;
        this.hostName = hostName;
        this.port = port;
        this.user = user;
        this.password = password;
        this.url=this.url.replace("${hostName}",hostName).replace("${port}",String.valueOf(port)).replace("${serviceName}",serviceName);
       try {
           connection = getConnection();
       }catch (Exception e){
           log.error("获取连接异常:{}",e);
       }
    }

    public  Connection getConnection() throws Exception{
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }

    public Set<OracleCdcSource> getCdcTableSet() throws Exception{
        statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(OracleMetadataConsts.META_PK_TABLE_LIST_SQL.replace("${databaseName}",databaseName));
        Set<OracleCdcSource> oracleCdcSourceSet = new HashSet<>();
        while (rs.next()){
            String table_name = rs.getString("table_name");
            OracleCdcSource oracleCdcSource = OracleCdcSource.builder()
                    .sourceSchema(databaseName)
                    .sourceTable(table_name)
                    .tableDesc(rs.getString("comments"))
                    .build();
            oracleCdcSourceSet.add(oracleCdcSource);
        }
        // 获取字段信息
        ResultSet rs2 = statement.executeQuery(OracleMetadataConsts.META_SCHEMA_SQL.replace("${databaseName}",databaseName));
        List<TableDesc> tableDescList = new ArrayList<>();
        while (rs2.next()){
            String column_comment = rs2.getString("column_comment");
            if(StringUtils.isNotBlank(column_comment)){
                column_comment=column_comment.replace("'","").replace(",","，");
            }

            TableDesc tableDesc = TableDesc.builder()
                    .tableName(rs2.getString("table_name"))
                    .columnName(rs2.getString("column_name"))
                    .columnType(rs2.getString("column_type"))
                    .columnComment(column_comment)
                    .tableComment(rs2.getString("table_comment"))
                    .isNull(rs2.getString("is_null"))
                    .isUniqueKey(rs2.getString("is_unique_key"))
                    .build();
            tableDescList.add(tableDesc);
        }

        ResultSet resultSet = statement.executeQuery(OracleMetadataConsts.ORACLE_LOG_DATA_TABLE_LIST_SQL.replace("${databaseName}", databaseName));
        List<String> log_table_list = new ArrayList<>();
        while (resultSet.next()){
            log_table_list.add(resultSet.getString("TABLE_NAME"));
        }

        Map<String, List<TableDesc>> collect = tableDescList.stream().collect(Collectors.groupingBy(TableDesc::getTableName));
        oracleCdcSourceSet.stream().map(cdc->{
            String replace = OracleMetadataConsts.ORACLE_LOG_DATA_ALTER_SQL.replace("${databaseName}", databaseName).replace("${tableName}", cdc.getSourceTable());
            try {
                if(!log_table_list.contains(cdc.getSourceTable())){
                    statement.execute(replace);
                }
            } catch (SQLException e) {
                log.error("执行SQL:{},发生异常{}",replace,e);
            }
            List<TableDesc> list = collect.get(cdc.getSourceTable());
            cdc.setDdl(list);
            return cdc;
        }).collect(Collectors.toList());

        statement.close();
        connection.close();
        return oracleCdcSourceSet;
    }




    public static void main(String[] args) throws Exception{
        OracleJdbcSourceFunction oracleJdbcSourceFunction = new OracleJdbcSourceFunction("WMS_DEV", "ORCLPDB1","172.31.3.67", 1521, "C##usr_dev_yangkaiyuan", "Sy_2023@yky");
        Set<OracleCdcSource> cdcTableSet = oracleJdbcSourceFunction.getCdcTableSet();

        System.out.println(cdcTableSet);


    }





}
