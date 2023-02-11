package com.ocean.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

/**
 * @author 徐正洲
 * @create 2023-01-05 9:58
 * <p>
 * 雷达数据UDP接收
 */
public class RadarDataSource extends RichSourceFunction<String> {
    private DatagramSocket datagramSocket = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        datagramSocket = new DatagramSocket(8899);
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (true) {
            //接收数据包
            byte[] buffer = new byte[1024];

            DatagramPacket datagramPacket = new DatagramPacket(buffer, 0, buffer.length);

            datagramSocket.receive(datagramPacket);

            String data = new String(datagramPacket.getData(), 0, datagramPacket.getLength());

            sourceContext.collect(data);
        }
    }

    @Override
    public void cancel() {
    }
}
