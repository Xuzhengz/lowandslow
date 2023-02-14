package com.ocean.app.ods;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.ocean.bean.RadarBean;
import com.ocean.utils.KafkaUtil;
import com.ocean.utils.MysqlUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 徐正洲
 * @create 2023-02-14 3:07
 * <p>
 * 雷达数据写入mysql记录
 */
public class RadarToMysql {
    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            String topic = "radar";
            String groupId = "radar_consumers";
            DataStreamSource<String> radarDs = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

            SingleOutputStreamOperator<RadarBean> radarObj = radarDs.flatMap(new FlatMapFunction<String, RadarBean>() {
                @Override
                public void flatMap(String s, Collector<RadarBean> collector) throws Exception {
                    try {
                        RadarBean radarBean = JSONObject.parseObject(s, RadarBean.class);
                        collector.collect(radarBean);
                    } catch (Exception e) {
                        System.out.println("radarObj算子转换bean异常：-->" + s + "\n" + "异常信息-->" + e.getMessage());
                    }

                }
            });

            //TODO 雷达数据写入mysql
            radarObj.addSink(MysqlUtil.getMysqlSink("insert into radar values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));
            env.execute();
        } catch (Exception e) {
            System.out.println("异常捕获--->" + e.getMessage());
        }
    }

}
