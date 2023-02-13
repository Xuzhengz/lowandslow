package com.ocean.function;

import com.ocean.utils.BaseSystemUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
        datagramSocket = new DatagramSocket(10005);
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (true) {

            //接收数据包
            byte[] buffer = new byte[1024];

            DatagramPacket datagramPacket = new DatagramPacket(buffer, 0, buffer.length);


            datagramSocket.receive(datagramPacket);

            //接收到的雷达报文字节数组
            byte[] byteData = datagramPacket.getData();

            //接收到的雷达报文数据长度
            int length = datagramPacket.getLength();

            //雷达报文数据转成十六进制
            String data = BaseSystemUtil.bytesToHexString(byteData, length);
            if (data.startsWith("c6c6")) {
                //输出雷达报文
                sourceContext.collect(data);
            }
        }
    }

    @Override
    public void cancel() {
    }

}
