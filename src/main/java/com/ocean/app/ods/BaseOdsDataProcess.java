package com.ocean.app.ods;

import com.ocean.function.ElectroMagneticDataSource;
import com.ocean.function.PhotoElectricDataSource;
import com.ocean.function.RadarDataSource;
import com.ocean.utils.KafkaUtil;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;

/**
 * @author 徐正洲
 * @create 2023-01-05 9:53
 */
public class BaseOdsDataProcess {
    public static void main(String[] args) throws Exception {
        //TODO 1、配置故障恢复
        /**
         * 启动时，还原至checkpoint的保存点
         */
//        Configuration configuration = new Configuration();
//
//        String savePath = "file://C://Users//bigbi//Desktop//ods_ck";
//        String ckFile = CkFileUtil.getMaxTimeFileName(new File(savePath));
//
//        if (ckFile != null && !"".equalsIgnoreCase(ckFile.trim())) {
//            configuration.setString("execution.savepoint.path", ckFile);
//        }

        //TODO 2、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //配置为Kafka的分区数
        env.setParallelism(1);

        /**
         * 设置状态后端
         */
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());
//        // 启用 checkpoint,设置触发间隔（两次执行开始时间间隔）
//        env.enableCheckpointing(10000);
////        模式支持EXACTLY_ONCE()/AT_LEAST_ONCE()
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
////        存储位置，FileSystemCheckpointStorage(文件存储)
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(savePath));
////        超时时间，checkpoint没在时间内完成则丢弃
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
////        同时并发数量
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
////        最小间隔时间（前一次结束时间，与下一次开始时间间隔）
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
//        //表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint

        //TODO 3、接入雷达、光电、电磁数据来源
        SingleOutputStreamOperator<String> radarSource = env.addSource(new DataGeneratorSource<>(new RadarDataSource())).returns(String.class);
        SingleOutputStreamOperator<String> photoElectricDataSource = env.addSource(new DataGeneratorSource<>(new PhotoElectricDataSource())).returns(String.class);
        SingleOutputStreamOperator<String> electroMagneticDataSource = env.addSource(new DataGeneratorSource<>(new ElectroMagneticDataSource())).returns(String.class);
        radarSource.print("雷达数据>>>");
        photoElectricDataSource.print("光电数据>>>");
        electroMagneticDataSource.print("电磁数据>>>");

        //TODO 4、写出到kafka_ods层
        String kafka_ods = "ods";
        radarSource.addSink(KafkaUtil.getFlinkKafkaProducer(kafka_ods));
        photoElectricDataSource.addSink(KafkaUtil.getFlinkKafkaProducer(kafka_ods));
        electroMagneticDataSource.addSink(KafkaUtil.getFlinkKafkaProducer(kafka_ods));

        env.execute();

    }
}
