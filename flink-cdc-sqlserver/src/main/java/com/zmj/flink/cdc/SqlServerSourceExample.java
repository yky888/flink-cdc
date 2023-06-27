package com.zmj.flink.cdc;

import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.catalog.CatalogBaseTable;

import java.util.Properties;
import java.util.UUID;


public class SqlServerSourceExample {

    public static void main(String[] args) throws Exception {

        //设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "root");

        SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
                .hostname("xxxxx")
                .port(1433)
                .database("xxxx") // monitor sqlserver database
                .tableList("dbo.nxjob") // monitor products table
                .username("xxxx")
                .password("xxx")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironmentBuilder.buildEnv(1);
        env.enableCheckpointing(10000L);



        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("xxxx:8030,xxxx:8030,xxxx:8030")
                .setTableIdentifier("tmp.nxjob")
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


        DataStreamSource<String> stringDataStreamSource = env.addSource(sourceFunction);

       // stringDataStreamSource.print();

        stringDataStreamSource.sinkTo(builder.build());

        //.sinkTo(builder.build());


        env.execute("1");

    }
}
