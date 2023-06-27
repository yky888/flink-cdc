package com.zmj.flink.common;

import com.zmj.flink.common.consts.FlinkConfConsts;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @BelongsProject: data-platform-parent
 * @BelongsPackage: com.zmj.flink.common
 * @Author: kaiyuanyang
 * @CreateTime: 2023-06-07  15:46
 * @Description: TODO
 * @Version: 1.0
 */
public class StreamExecutionEnvironmentBuilder {


    public static StreamExecutionEnvironment buildEnv(int parallelism) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(parallelism);
        //检查点间隔
        env.enableCheckpointing(1000 * 10, CheckpointingMode.EXACTLY_ONCE);
        //env.disableOperatorChaining();
        //设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //指定从CK自动重启策略
      //  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        //确保检查点之间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60000 * 3);
        //同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
       // env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        //设置状态存储
        //env.setStateBackend(new RocksDBStateBackend(FlinkConfConsts.FLINK_CHECK_POINTS_PATH,true));
        //rocksdb 存储
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().setCheckpointStorage(FlinkConfConsts.FLINK_CHECK_POINTS_PATH);
        return env;
    }



}
