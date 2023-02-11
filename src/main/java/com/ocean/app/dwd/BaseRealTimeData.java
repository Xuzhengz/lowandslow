package com.ocean.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.ocean.utils.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author 徐正洲
 * @create 2023-01-05 9:54
 */
public class BaseRealTimeData {
    public static void main(String[] args) throws Exception {
        //TODO 1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //配置为Kafka的分区数
        env.setParallelism(1);
        //TODO 2、配置故障恢复
        /**
         * 启动时，还原至checkpoint的保存点
         */
//        Configuration configuration = new Configuration();
//
//        String savePath = "file://C://Users//bigbi//Desktop//dwd_ck";
//        String ckFile = CkFileUtil.getMaxTimeFileName(new File(savePath));
//
//        if (ckFile != null && !"".equalsIgnoreCase(ckFile.trim())) {
//            configuration.setString("execution.savepoint.path", ckFile);
//        }

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

        //TODO 3、消费ods层数据
        String topic = "ods";
        String group = "dwd";

        DataStreamSource<String> odsSource = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, group));
        //TODO 4、分流处理雷达、光电、电磁数据

        OutputTag<String> dirtyTag = new OutputTag<String>("dirty") {
        };

        SingleOutputStreamOperator<JSONObject> jsonObjDs = odsSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }
            }
        });
        DataStream<String> drityDs = jsonObjDs.getSideOutput(dirtyTag);
        drityDs.print("脏数据>>>");

        //TODO 5、数据筛选
        SingleOutputStreamOperator<String> rsDs = jsonObjDs.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject jsonObject) throws Exception {
                String sourceType = jsonObject.getString("SourceType");
                if (sourceType.equals("radar")) {
                    jsonObject.remove("TargetEchoAmplitude");
                    jsonObject.remove("TargetXAxisDistance");
                    jsonObject.remove("TargetYAxisDistance");
                    jsonObject.remove("TargetZAxisDistance");
                    jsonObject.remove("CourseOfTheTarget");
                    jsonObject.remove("TargetSpeed");
                    jsonObject.remove("TargetX-axisSpeed");
                    jsonObject.remove("TargetY-axisSpeed");
                    jsonObject.remove("TargetZ-axisSpeed");
                    jsonObject.remove("TargetLotNumber");
                    return jsonObject.toJSONString();
                } else if (sourceType.equals("electromagnetism")) {
                    jsonObject.remove("TargetXAxisDistance");
                    jsonObject.remove("TargetYAxisDistance");
                    jsonObject.remove("TargetZAxisDistance");
                    return jsonObject.toJSONString();
                } else {
                    jsonObject.remove("TargetXAxisDistance");
                    jsonObject.remove("TargetYAxisDistance");
                    jsonObject.remove("TargetZAxisDistance");
                    return jsonObject.toJSONString();
                }
            }
        });

        //TODO 5、写出kakfa_dwd层
        String rsTopic = "dwd";
        rsDs.addSink(KafkaUtil.getFlinkKafkaProducer(rsTopic));

        env.execute();
    }
}