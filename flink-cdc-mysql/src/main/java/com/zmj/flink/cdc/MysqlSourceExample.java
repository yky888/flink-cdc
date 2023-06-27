package com.zmj.flink.cdc;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.zmj.flink.common.StreamExecutionEnvironmentBuilder;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.JsonDebeziumSchemaSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class MysqlSourceExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironmentBuilder.buildEnv(1);

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        Map<String, Object> customConverterConfigs = new HashMap<>();
        //TODO 4.使用FlinkCDC读取MySQL配置信息表创建配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("xxxx")
                .port(3306)
                .username("xxx")
                .password("xxxx")
                .databaseList("test")
                .tableList("test.student")
                //.startupOptions(StartupOptions.initial())    //初始化  重头读取
                .serverTimeZone("Asia/Shanghai")
                .includeSchemaChanges(true) // converts SourceRecord to JSON String
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MysqlSource");

        /*SingleOutputStreamOperator<JSONObject> resultDS = mysqlSourceDS.map(str -> {
            return JSONObject.parseObject(str);
        });*/
        //resultDS.addSink(new DorisSinkFunction());

        // enable checkpoint
        env.enableCheckpointing(10000);

        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        props.setProperty("line_delimiter", "\n");
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("xxxx:8030,xxxx:8030,xxxx:8030")
                .setTableIdentifier("tmp.student")
                .setUsername("xxx")
                .setPassword("xxxx").build();

        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-doris" + UUID.randomUUID())
                .setStreamLoadProp(props).setDeletable(true);

        DorisSink.Builder<String> builder = DorisSink.builder();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setDorisOptions(dorisOptions)
                .setSerializer(JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOptions).build());

      /*  SingleOutputStreamOperator<String> resultDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source").map(str -> {
            return JSONObject.parseObject(str).getString("data");
        });*/
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")//.print();
                .sinkTo(builder.build());

        /*resultDS.print();
        resultDS.sinkTo(builder.build());*/
        //打印数据
        mysqlSourceDS.printToErr("------>");


        env.execute("mysqlCDC");
    }
}
