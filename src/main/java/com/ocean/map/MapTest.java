package com.ocean.map;

import com.ocean.common.DxmConfig;
import com.ocean.function.RadarDataSource;
import com.ocean.utils.KafkaUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author 徐正洲
 * @create 2023-02-15 13:31
 */
public class MapTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> radarTextDs = env.readTextFile("/radar.txt");

        String topic = "radar";
        radarTextDs.addSink(KafkaUtil.getFlinkKafkaProducer(topic));

        env.execute();
    }
}
