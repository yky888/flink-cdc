package com.zmj.flink.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.zmj.flink.cdc.entity.SqlServerCdcSource;
import com.zmj.flink.cdc.function.SqlServerJdbcSourceFunction;
import com.zmj.flink.cdc.utils.SqlServerConnectionUtils;
import com.zmj.flink.common.StreamExecutionEnvironmentBuilder;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.jdbc.JdbcConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.JsonDebeziumSchemaSerializer;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class SqlServerDatabaseExample {

    public static void main(String[] args) throws Exception {

        //设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "root");

        SqlServerJdbcSourceFunction sqlServerJdbcSourceFunction = new SqlServerJdbcSourceFunction("xxxx", "xxxxx", 1433, "xxx", "xxxx");
        Set<SqlServerCdcSource> cdcTableSet = sqlServerJdbcSourceFunction.getCdcTableSet();

        List<String> collect = cdcTableSet.stream().map(obj -> {
            String sourceSchema = obj.getSourceSchema();
            String sourceTable = obj.getSourceTable();
            return sourceSchema + "." + sourceTable;
        }).collect(Collectors.toList());


        SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
                .hostname("xxxx")
                .port(1433)
                .database("xxxx") // monitor sqlserver database
                .tableList(String.join(",",collect)) // monitor products table
                .username("xxx")
                .password("xxxx")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //加载数据源
        DataStreamSource<String> stringDataStreamSource = env.addSource(sourceFunction);



        List<String> sinkList = cdcTableSet.stream().map(obj -> {
            String sourceTable = obj.getSourceTable();
            return sourceTable;
        }).collect(Collectors.toList());


        for(String tbl:sinkList) {
            SingleOutputStreamOperator<String> filterStream = filterTableData(stringDataStreamSource, tbl);
            SingleOutputStreamOperator<String> cleanStream = clean(filterStream);
            DorisSink dorisSink = buildDorisSink(tbl);
            cleanStream.sinkTo(dorisSink).name("sink " + tbl);
        }


        env.execute("sqlserver cdc job");

    }




    private static SingleOutputStreamOperator<String> clean(SingleOutputStreamOperator<String> source) {
        return source.flatMap(new FlatMapFunction<String,String>(){
            @Override
            public void flatMap(String row, Collector<String> out) throws Exception {
                try{
                    JSONObject rowJson = JSON.parseObject(row);
                    String op = rowJson.getString("op");
                    if (Arrays.asList("c", "r", "u").contains(op)) {
                        JSONObject after = rowJson.getJSONObject("after");
                        after.put("__DORIS_DELETE_SIGN__", 0);
                        out.collect(after.toJSONString());
                    } else if ("d".equals(op)) {
                        JSONObject before = rowJson.getJSONObject("before");
                        before.put("__DORIS_DELETE_SIGN__", 1);
                        out.collect(before.toJSONString());
                    } else {
                        log.info("filter other op:{}", op);
                    }
                }catch (Exception ex){
                    log.warn("filter other format binlog:{}",row);
                }
            }
        });
    }



    /**
     * Divide according to tablename
     * */
    private static SingleOutputStreamOperator<String> filterTableData(DataStreamSource<String> source, String table) {
        return source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String row) throws Exception {
                try {
                    JSONObject rowJson = JSON.parseObject(row);
                    JSONObject source = rowJson.getJSONObject("source");
                    String tbl = source.getString("table");
                    return table.equals(tbl);
                }catch (Exception ex){
                    ex.printStackTrace();
                    return false;
                }
            }
        });
    }


    /**
     * create doris sink
     * */
    public static DorisSink<String> buildDorisSink(String table){
        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("172.31.3.87:8030,172.31.3.88:8030,172.31.3.89:8030")
                .setTableIdentifier("zmj_ods_spxssc_cdc" + "." + table)
                .setUsername("root")
                .setPassword("root@123");

        Properties pro = new Properties();
        //json data format
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setLabelPrefix("label-" + table + UUID.randomUUID()) //streamload label prefix,
                .disable2PC()
                .setStreamLoadProp(pro)
                .setDeletable(true)
                .build();

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer()) //serialize according to string
                .setDorisOptions(dorisBuilder.build());

        return builder.build();
    }

}
