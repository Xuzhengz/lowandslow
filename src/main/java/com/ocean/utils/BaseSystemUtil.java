package com.ocean.utils;

import org.apache.flink.table.planner.expressions.In;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


/**
 * @author 徐正洲
 * @create 2023-02-13 10:33
 */
public class BaseSystemUtil {
    //大小端转换，十六进制转换成十进制
    public static String baseSystemTransfer(String message) {
        String data = null;
        //如果字符串长度为4，两两转换
        if (message.length() == 4) {
            String s1 = message.substring(0, 2);
            String s2 = message.substring(2, 4);
            data = s2 + s1;
        }
        //如果字符串长度为8，每4个为一组，组内两两转换后，组与组之间互换
        if (message.length() == 8) {
            String s1 = message.substring(0, 2);   // e2
            String s2 = message.substring(2, 4);   // 06
            String s3 = message.substring(4, 6);   // 00
            String s4 = message.substring(6, 8);   // 00
            data = s4 + s3 + s2 + s1;              // 000006e2
        }

        String msg = String.valueOf(Long.parseLong(data, 16));

        return msg;
    }


    /* *

     * Convert byte[] to hex string.这里我们可以将byte转换成int，然后利用Integer.toHexString(int)

     *来转换成16进制字符串。

     * @param src byte[] data

     * @return hex string

     */

    public static String bytesToHexString(byte[] src, Integer byteLength) {

        StringBuilder stringBuilder = new StringBuilder("");

        if (src == null || src.length <= 0) {

            return null;

        }

        for (int i = 0; i < byteLength; i++) {

            int v = src[i] & 0xFF;

            String hv = Integer.toHexString(v);

            if (hv.length() < 2) {

                stringBuilder.append(0);

            }

            stringBuilder.append(hv);

        }

        return stringBuilder.toString();

    }


}
