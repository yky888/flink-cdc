package com.zmjsy.flink.cdc.tools.oracle;

import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

/**
 * @BelongsProject: data-platform-parent
 * @BelongsPackage: com.zmjsy.flink.cdc.tools.oracle
 * @Author: kaiyuanyang
 * @CreateTime: 2023-06-24  16:03
 * @Description: TODO
 * @Version: 1.0
 */
public class OracleCDCTest {

    public static void main(String[] args) throws Exception{
        System.setProperty("HADOOP_USER_NAME", "root");

        Properties properties = new Properties();
        properties.put("database.dbname","SPXSSC");
        properties.put("database.pdb.name","ORCLPDB1");


        DebeziumSourceFunction<String> build = OracleSource
                .<String>builder()
                .hostname("172.31.3.67")
                .port(1521)
                .url("jdbc:oracle:thin:@172.31.3.67:1521/ORCLPDB1")
                .database("SPXSSC")
                .username("C##usr_dev_yangkaiyuan")
                .password("Sy_2023@yky")
                .tableList("spxssc.nxjob")
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000);

        env.addSource(build).print();


        env.execute("1111");
//
//        SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
//                .hostname("172.31.3.96")
//                .port(1433)
//                .database("spxboot") // monitor sqlserver database
//                .tableList(String.join(",",collect)) // monitor products table
//                .username("sa")
//                .password("Aa123456")
//                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .build();
    }

}
