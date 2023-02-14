package com.ocean.app.ods;

import com.alibaba.fastjson.JSONObject;
import com.ocean.function.RadarDataSource;
import com.ocean.function.TargetDataSource;
import com.ocean.utils.BaseSystemUtil;
import com.ocean.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class UdpToKafka {
    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //设置并行度--kafka的分区数
            env.setParallelism(1);
            //TODO 预置操作，可开启CK检查点保存。
//        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
//        设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall-flink/ck");
//        System.setProperty("HADOOP_USER_NAME","root");

            //TODO 1、UDP接收雷达报文与目标类型报文    雷达报文端口：10005  目标类型报文端口：10006
            DataStreamSource<String> radarDs = env.addSource(new RadarDataSource(), "雷达报文");
            DataStreamSource<String> targetDs = env.addSource(new TargetDataSource(), "目标类型报文");


            //TODO 2、雷达与目标类型报文十六进制转十进制（时戳：从今天0点开始算，截至到今天24点）封装成bean
            SingleOutputStreamOperator<JSONObject> radarTransDs = radarDs.flatMap(new FlatMapFunction<String, JSONObject>() {
                @Override
                public void flatMap(String data, Collector<JSONObject> collector) {
                    JSONObject radarJson = new JSONObject();
                    radarJson.put("msgId", BaseSystemUtil.baseSystemTransfer(data.substring(0, 4)));
                    radarJson.put("msgLength", BaseSystemUtil.baseSystemTransfer(data.substring(4, 8)));
                    radarJson.put("msgFrameNum", BaseSystemUtil.baseSystemTransfer(data.substring(8, 16)));
//                    radarJson.put("ts", DateFormatUtil.getTs(BaseSystemUtil.baseSystemTransfer(data.substring(16, 24))));
                    radarJson.put("ts", System.currentTimeMillis());
                    radarJson.put("targetType", "4");
                    radarJson.put("dataPeriod", BaseSystemUtil.baseSystemTransfer(data.substring(32, 36)));
                    radarJson.put("targetEcho", BaseSystemUtil.baseSystemTransfer(data.substring(36, 40)));
                    radarJson.put("targetHeight", BaseSystemUtil.baseSystemTransfer(data.substring(40, 48)));
                    radarJson.put("bearing", BaseSystemUtil.baseSystemTransfer(data.substring(48, 52)));
                    radarJson.put("elevation", BaseSystemUtil.baseSystemTransfer(data.substring(52, 56)));
                    radarJson.put("targetXWheelbaseIsland", BaseSystemUtil.baseSystemTransfer(data.substring(56, 64)));
                    radarJson.put("targetYAxisBaseHeight", BaseSystemUtil.baseSystemTransfer(data.substring(64, 72)));
                    radarJson.put("targetZAxisBaseHeight", BaseSystemUtil.baseSystemTransfer(data.substring(72, 80)));
                    radarJson.put("preHeight", BaseSystemUtil.baseSystemTransfer(data.substring(80, 88)));
                    radarJson.put("preBearing", BaseSystemUtil.baseSystemTransfer(data.substring(88, 92)));
                    radarJson.put("preElevation", BaseSystemUtil.baseSystemTransfer(data.substring(92, 96)));
                    radarJson.put("courseOfTheTarget", BaseSystemUtil.baseSystemTransfer(data.substring(96, 100)));
                    radarJson.put("targetSpeed", BaseSystemUtil.baseSystemTransfer(data.substring(100, 104)));
                    radarJson.put("reserveCol1", BaseSystemUtil.baseSystemTransfer(data.substring(104, 108)));
                    radarJson.put("reserveCol2", BaseSystemUtil.baseSystemTransfer(data.substring(108, 112)));
                    radarJson.put("targetX_AxisSpeed", BaseSystemUtil.baseSystemTransfer(data.substring(112, 116)));
                    radarJson.put("targetY_AxisSpeed", BaseSystemUtil.baseSystemTransfer(data.substring(116, 120)));
                    radarJson.put("internationalZ_Axis", BaseSystemUtil.baseSystemTransfer(data.substring(120, 124)));
                    radarJson.put("targetBatchNum", BaseSystemUtil.baseSystemTransfer(data.substring(124, 128)));
                    radarJson.put("messageStatus", BaseSystemUtil.baseSystemTransfer(data.substring(128, 132)));

                    collector.collect(radarJson);
                }
            }).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                @Override
                public long extractTimestamp(JSONObject jsonObject, long l) {
                    return jsonObject.getLong("ts");
                }
            }));

            radarTransDs.print("雷达实体--->");


            SingleOutputStreamOperator<JSONObject> targetTypeDs = targetDs.flatMap(new FlatMapFunction<String, JSONObject>() {
                @Override
                public void flatMap(String data, Collector<JSONObject> collector) throws Exception {
                    try {
                        String[] fields = data.split(",");
                        JSONObject targetJson = new JSONObject();
                        targetJson.put("msgHead", fields[0]);
                        targetJson.put("targetBatchNum", fields[1]);
                        targetJson.put("targetType", fields[2]);
                        targetJson.put("confidence", fields[3]);
                        targetJson.put("ts", fields[4]);
                        collector.collect(targetJson);
                    } catch (Exception e) {
                        System.out.println("targetTypeDs算子异常：--->" + e.getMessage());
                    }
                }
            }).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                @Override
                public long extractTimestamp(JSONObject jsonObject, long l) {
                    return jsonObject.getLong("ts");
                }
            }));

            targetTypeDs.print("目标类型实体-->");

            //TODO 3、将雷达与目标类型报文合流，如果雷达批号与报文批号一致，将目标类型值设置到雷达实体的目标类型属性
            SingleOutputStreamOperator<String> unionDS = radarTransDs.keyBy(new KeySelector<JSONObject, String>() {
                        @Override
                        public String getKey(JSONObject jsonObject) throws Exception {
                            return jsonObject.getString("targetBatchNum");
                        }
                    }).intervalJoin(targetTypeDs.keyBy(new KeySelector<JSONObject, String>() {
                        @Override
                        public String getKey(JSONObject jsonObject) throws Exception {
                            return jsonObject.getString("targetBatchNum");
                        }
                    })).between(Time.seconds(-2), Time.seconds(2))
                    .process(new ProcessJoinFunction<JSONObject, JSONObject, String>() {
                        @Override
                        public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, String>.Context context, Collector<String> collector) {
                            String targetBatchNum = jsonObject.getString("targetBatchNum");
                            String targetBatchNum2 = jsonObject2.getString("targetBatchNum");

                            if (targetBatchNum.equals(targetBatchNum2)){
                                try {
                                    jsonObject.put("targetType",jsonObject2.getString("targetType"));
                                    collector.collect(jsonObject.toJSONString());
                                }catch (Exception e){
                                    System.out.println("unionDS算子异常-->" + e.getMessage());
                                }

                            }
                        }
                    });


            unionDS.print("result--->");
//

            //TODO 4、雷达数据写入Kafka
            String radar_topic = "radar";
            unionDS.addSink(KafkaUtil.getFlinkKafkaProducer(radar_topic));


            //TODO 5、启动
            env.execute();
        } catch (Exception e) {
            System.out.println("异常捕获--->" + e.getMessage());
        }
    }
}
