package com.zmj.flink.cdc.function;

import com.zmj.flink.cdc.entity.SqlServerCdcSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Set;

/**
 * @BelongsProject: data-platform-parent
 * @BelongsPackage: com.zmj.flink.cdc.function
 * @Author: kaiyuanyang
 * @CreateTime: 2023-06-09  17:10
 * @Description: TODO
 * @Version: 1.0
 */
@Slf4j
public class DorisJdbcSourceFunction {

    private String databaseName;

    private String hostName;

    private int port = 9030;

    private String user;

    private String password;

    private  String url = "jdbc:mysql://${hostName}:${port}/${databaseName}";

    private Connection connection;

    private Statement statement;


    public DorisJdbcSourceFunction() {
    }

    public DorisJdbcSourceFunction(String databaseName, String hostName, int port, String user, String password) {
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
        return conn;
    }

    public static void main(String[] args) {

        SqlServerCdcSource a = null;
       try {

        DorisJdbcSourceFunction dorisJdbcSourceFunction = new DorisJdbcSourceFunction("zmj_ods_spxssc_cdc", "172.31.3.76", 9030, "root", "root@123");
        Connection connection = dorisJdbcSourceFunction.getConnection();
        Statement statement = connection.createStatement();
        SqlServerJdbcSourceFunction sqlServerJdbcSourceFunction = new SqlServerJdbcSourceFunction("spxssc", "172.31.3.96", 1433, "sa", "Aa123456");
        Set<SqlServerCdcSource> cdcTableSet = sqlServerJdbcSourceFunction.getCdcTableSet();
       // int i =1;
       for(SqlServerCdcSource sqlServerCdcSource:cdcTableSet){
           a = sqlServerCdcSource;
           boolean execute = statement.execute(sqlServerCdcSource.getDorisDdl());
          // if(execute){
               log.info("sqlserver 源表 {}，doris 创表成功",sqlServerCdcSource.getSourceTable());
               System.out.println("sqlserver 源表 {}，doris 创表成功"+sqlServerCdcSource.getSourceTable());
        //   }
       }
        statement.close();
       connection.close();
      //  ResultSet rs = statement.executeQuery("SHOW PROC '/frontends'");

    }catch (Exception e){
           System.out.println(a);
       }


    }



}
