package com.ocean.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;

/**
 * @author 徐正洲
 * @create 2023-01-05 9:58
 * <p>
 * 雷达数据模拟
 */
public class RadarDataSource extends RandomGenerator<String> {
    private RandomDataGenerator random = new RandomDataGenerator();
    private JSONObject json = new JSONObject();


    @Override
    public String next() {
        try {
            json.put("TargetEchoAmplitude", random.nextInt(0, 255));
            json.put("TargetDistance", random.nextInt(0, 12000));
            json.put("TargetBearing", random.nextInt(0, 36000));
            json.put("TargetElevation", random.nextInt(-100, 3000));
            json.put("TargetXAxisDistance", random.nextInt(-12000, 12000));
            json.put("TargetYAxisDistance", random.nextInt(-12000, 12000));
            json.put("TargetZAxisDistance", random.nextInt(-12000, 12000));
            json.put("PredictionDistance", random.nextInt(0, 12000));
            json.put("PredictionBearing", random.nextInt(0, 36000));
            json.put("PredictionElevation", random.nextInt(-100, 3000));
            json.put("CourseOfTheTarget", random.nextInt(0, 36000));
            json.put("TargetSpeed", random.nextInt(0, 2000));
            json.put("TargetX-axisSpeed", random.nextInt(-2000, 2000));
            json.put("TargetY-axisSpeed", random.nextInt(-2000, 2000));
            json.put("TargetZ-axisSpeed", random.nextInt(-2000, 2000));
            json.put("TargetLotNumber", random.nextInt(6000, 7999));
            json.put("TrackingStatusMessage", random.nextInt(0, 4));
            json.put("TargetType", "空飘物体待确认");
            json.put("Ts", System.currentTimeMillis());
            json.put("SourceType", "radar");
            Thread.sleep(2000L);
            return json.toJSONString();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
