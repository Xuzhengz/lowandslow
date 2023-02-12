package com.ocean.app.ods;

import com.ocean.function.RadarDataSource;
import com.ocean.utils.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class UdpToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<String> radarDs = env.addSource(new RadarDataSource(), "雷达数据");
        radarDs.print("radar::::");

        String radar_topic = "radar";
        radarDs.addSink(KafkaUtil.getFlinkKafkaProducer(radar_topic));

        env.execute();
    }
}
