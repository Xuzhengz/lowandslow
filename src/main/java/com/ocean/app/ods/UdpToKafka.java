package com.ocean.app.ods;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.ocean.bean.RadarBean;
import com.ocean.bean.TargetType;
import com.ocean.function.RadarDataSource;
import com.ocean.function.TargetDataSource;
import com.ocean.utils.BaseSystemUtil;
import com.ocean.utils.DateFormatUtil;
import com.ocean.utils.KafkaUtil;
import com.ocean.utils.MysqlUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

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
                public void flatMap(String data, Collector<JSONObject> collector) throws Exception {
                    JSONObject radarJson = new JSONObje

                    RadarBean radarBean = RadarBean.builder()
                            .msgId(BaseSystemUtil.baseSystemTransfer(data.substring(0, 4)))
                            .msgLength(BaseSystemUtil.baseSystemTransfer(data.substring(4, 8)))
                            .msgFrameNum(BaseSystemUtil.baseSystemTransfer(data.substring(8, 16)))
                            .ts(DateFormatUtil.getTs(BaseSystemUtil.baseSystemTransfer(data.substring(16, 24))))
                            .targetType("4")
                            .dataPeriod(BaseSystemUtil.baseSystemTransfer(data.substring(32, 36)))
                            .targetEcho(BaseSystemUtil.baseSystemTransfer(data.substring(36, 40)))
                            .targetHeight(BaseSystemUtil.baseSystemTransfer(data.substring(40, 48)))
                            .bearing(BaseSystemUtil.baseSystemTransfer(data.substring(48, 52)))
                            .elevation(BaseSystemUtil.baseSystemTransfer(data.substring(52, 56)))
                            .targetXWheelbaseIsland(BaseSystemUtil.baseSystemTransfer(data.substring(56, 64)))
                            .targetYAxisBaseHeight(BaseSystemUtil.baseSystemTransfer(data.substring(64, 72)))
                            .targetZAxisBaseHeight(BaseSystemUtil.baseSystemTransfer(data.substring(72, 80)))
                            .preHeight(BaseSystemUtil.baseSystemTransfer(data.substring(80, 88)))
                            .preBearing(BaseSystemUtil.baseSystemTransfer(data.substring(88, 92)))
                            .preElevation(BaseSystemUtil.baseSystemTransfer(data.substring(92, 96)))
                            .courseOfTheTarget(BaseSystemUtil.baseSystemTransfer(data.substring(96, 100)))
                            .targetSpeed(BaseSystemUtil.baseSystemTransfer(data.substring(100, 104)))
                            .reserveCol1(BaseSystemUtil.baseSystemTransfer(data.substring(104, 108)))
                            .reserveCol2(BaseSystemUtil.baseSystemTransfer(data.substring(108, 112)))
                            .targetX_AxisSpeed(BaseSystemUtil.baseSystemTransfer(data.substring(112, 116)))
                            .targetY_AxisSpeed(BaseSystemUtil.baseSystemTransfer(data.substring(116, 120)))
                            .internationalZ_Axis(BaseSystemUtil.baseSystemTransfer(data.substring(120, 124)))
                            .targetBatchNum(BaseSystemUtil.baseSystemTransfer(data.substring(124, 128)))
                            .messageStatus(BaseSystemUtil.baseSystemTransfer(data.substring(128, 132)))
                            .build();

                    collector.collect(radarBean);
                }
            }).assignTimestampsAndWatermarks(WatermarkStrategy.<RadarBean>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<RadarBean>() {
                @Override
                public long extractTimestamp(RadarBean radarBean, long l) {
                    return radarBean.getTs();
                }
            }));

            radarTransDs.print("雷达实体--->");


            SingleOutputStreamOperator<JSONObject> targetTypeDs = targetDs.flatMap(new FlatMapFunction<String, JSONObject>() {
                @Override
                public void flatMap(String data, Collector<JSONObject> collector) throws Exception {
                    try {
                        String[] fields = data.split(",");
                        JSONObject targetJson = new JSONObject();
                        targetJson.put("msgHead",fields[0]);
                        targetJson.put("targetBatchNum",fields[1]);
                        targetJson.put("targetType",fields[2]);
                        targetJson.put("confidence",fields[3]);
                        targetJson.put("ts",fields[4]);
                        collector.collect(targetJson);
                    } catch (NumberFormatException e) {
                        System.out.println("数据类型转换失败--->" + data);
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
            SingleOutputStreamOperator<String> resultDs = radarTransDs.keyBy(RadarBean::getTargetBatchNum).intervalJoin(targetTypeDs.keyBy(TargetType::getTargetBatchNum)).between(Time.seconds(-5), Time.seconds(5)).process(new ProcessJoinFunction<RadarBean, TargetType, String>() {
                @Override
                public void processElement(RadarBean radarBean, TargetType targetType, ProcessJoinFunction<RadarBean, TargetType, String>.Context context, Collector<String> collector) {
                    try {
                        if (radarBean.getTargetBatchNum().equals(targetType.getTargetBatchNum())) {
                            radarBean.setTargetType(targetType.getTargetType());
                        }
                        Gson gson = new Gson();
                        String data = gson.toJson(radarBean);
                        collector.collect(data);
                    } catch (Exception e) {
                        System.out.println("异常捕获-->" + e.getMessage());
                    }

                }
            });

            resultDs.print("result--->");


            //TODO 4、雷达数据写入Kafka
            String radar_topic = "radar";
            resultDs.addSink(KafkaUtil.getFlinkKafkaProducer(radar_topic));


            env.execute();
        } catch (Exception e) {
            System.out.println("异常捕获--->" + e.getMessage());
        }
    }
}
