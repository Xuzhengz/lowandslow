package com.ocean.app.ods;

import cn.hutool.core.lang.Snowflake;
import cn.hutool.core.util.IdUtil;
import com.alibaba.fastjson.JSONObject;
import com.ocean.function.RadarDataSource;
import com.ocean.function.TargetDataSource;
import com.ocean.utils.BaseSystemUtil;
import com.ocean.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.time.Duration;

public class UdpToKafka {
    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //设置并行度--kafka的分区数
            env.setParallelism(3);
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

            radarDs.print("radar16进制-->");


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
                    radarJson.put("bearing", BaseSystemUtil.baseSystemTransfer(data.substring(48, 52)) / 100.00);
                    radarJson.put("elevation", BaseSystemUtil.baseSystemTransfer(data.substring(52, 56)) / 100.00);
                    radarJson.put("targetXWheelbaseIsland", BaseSystemUtil.baseSystemTransfer(data.substring(56, 64)));
                    radarJson.put("targetYAxisBaseHeight", BaseSystemUtil.baseSystemTransfer(data.substring(64, 72)));
                    radarJson.put("targetZAxisBaseHeight", BaseSystemUtil.baseSystemTransfer(data.substring(72, 80)));
                    radarJson.put("preHeight", BaseSystemUtil.baseSystemTransfer(data.substring(80, 88)));
                    radarJson.put("preBearing", BaseSystemUtil.baseSystemTransfer(data.substring(88, 92)) / 100.00);
                    radarJson.put("preElevation", BaseSystemUtil.baseSystemTransfer(data.substring(92, 96)) / 100.00);
                    radarJson.put("courseOfTheTarget", BaseSystemUtil.baseSystemTransfer(data.substring(96, 100)) / 100.00);
                    radarJson.put("targetSpeed", BaseSystemUtil.baseSystemTransfer(data.substring(100, 104)) / 10.00);
                    radarJson.put("reserveCol1", BaseSystemUtil.baseSystemTransfer(data.substring(104, 108)));
                    radarJson.put("reserveCol2", BaseSystemUtil.baseSystemTransfer(data.substring(108, 112)));
                    radarJson.put("targetX_AxisSpeed", BaseSystemUtil.baseSystemTransfer(data.substring(112, 116)) / 10.00);
                    radarJson.put("targetY_AxisSpeed", BaseSystemUtil.baseSystemTransfer(data.substring(116, 120)) / 10.00);
                    radarJson.put("internationalZ_Axis", BaseSystemUtil.baseSystemTransfer(data.substring(120, 124)) / 10.00);
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


            //TODO 3、将雷达与目标类型报文合流，如果雷达批号与报文批号一致，将目标类型值设置到雷达实体的目标类型属性
            SingleOutputStreamOperator<JSONObject> unionDS = radarTransDs.keyBy(new KeySelector<JSONObject, String>() {
                @Override
                public String getKey(JSONObject jsonObject) throws Exception {
                    return jsonObject.getString("targetBatchNum");
                }
            }).intervalJoin(targetTypeDs.keyBy(new KeySelector<JSONObject, String>() {
                @Override
                public String getKey(JSONObject jsonObject) throws Exception {
                    return jsonObject.getString("targetBatchNum");
                }
            })).between(Time.seconds(-2), Time.seconds(0)).process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                @Override
                public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) {
                    String targetBatchNum = jsonObject.getString("targetBatchNum");
                    String targetBatchNum2 = jsonObject2.getString("targetBatchNum");

                    if (targetBatchNum.equals(targetBatchNum2)) {
                        try {
                            jsonObject.put("targetType", jsonObject2.getString("targetType"));
                            // 丢弃不明物体数据
                            if (!jsonObject.getString("targetType").equals("4")) {
                                collector.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            System.out.println("unionDS算子异常-->" + e.getMessage());
                        }

                    }
                }
            });


            unionDS.print("join--->");

            //TODO 4、按批号分组，每个批号生成唯一标识进行对物体的轨迹跟踪
            SingleOutputStreamOperator<String> resultRemarkDs = unionDS.keyBy(data -> data.getString("targetBatchNum"))
                    .process(new KeyedProcessFunction<String, JSONObject, String>() {
                //创建状态用于给同批号的物体进行赋值
                private ValueState<String> remarkValue;

                private Snowflake snowflake;

                @Override
                public void open(Configuration parameters) throws Exception {
                    remarkValue = getRuntimeContext().getState(new ValueStateDescriptor<String>("remarkValue", String.class));
                    snowflake = IdUtil.getSnowflake(1, 1);
                }

                @Override
                public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                    //获取状态的值，如果值为空赋一个随机值给状态用于给json的remark赋值
                    String value = remarkValue.value();
                    if (value != null) {
                        if (!"3".equals(jsonObject.getString("messageStatus"))){
                            jsonObject.put("remark", value);
                        }else {
                            //如果报文状态如果为3，说明航迹丢失，不用在后续同批号插入相同的remark
                            jsonObject.put("remark", value);
                            //清空状态
                            remarkValue.clear();
                        }
                    } else {
                        //如果状态为空，生成唯一标识赋值给雷达实体，并且状态更新
                        String remark = String.valueOf(snowflake.nextId());
                        jsonObject.put("remark", remark);
                        //更新状态
                        remarkValue.update(remark);
                    }
                    collector.collect(jsonObject.toJSONString());
                }
            });


            //TODO 5、雷达数据写入Kafka
            String radar_topic = "radar";
            resultRemarkDs.addSink(KafkaUtil.getFlinkKafkaProducer(radar_topic));


            //TODO 6、启动
            env.execute();
        } catch (Exception e) {
            System.out.println("异常捕获--->" + e.getMessage());
        }
    }
}
