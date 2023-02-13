package com.ocean.function;

import com.ocean.utils.BaseSystemUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

/**
 * @author 徐正洲
 * @create 2023-02-13 10:17
 */
public class TargetDataSource extends RichSourceFunction<String> {

    private DatagramSocket datagramSocket = null;


    @Override
    public void open(Configuration parameters) throws Exception {
        datagramSocket = new DatagramSocket(10006);
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (true) {
            //接收数据包
            byte[] buffer = new byte[31];

            DatagramPacket datagramPacket = new DatagramPacket(buffer, 0, buffer.length);

            datagramSocket.receive(datagramPacket);

            String data = new String(datagramPacket.getData(), 0, datagramPacket.getData().length);


            //输出目标类型报文
            sourceContext.collect(data);

        }
    }

    @Override
    public void cancel() {
    }
}
