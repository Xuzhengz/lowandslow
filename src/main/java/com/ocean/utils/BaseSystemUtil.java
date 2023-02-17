package com.ocean.utils;

/**
 * @author 徐正洲
 * @create 2023-02-13 10:33
 */
public class BaseSystemUtil {
    //大小端转换，十六进制转换成十进制
    public static int baseSystemTransfer(String message) {
        //如果字符串长度为4，两两转换
        if (message.length() == 4) {
            return ByteConvertUtil.parse(message, ByteConvertUtil.SHORT);
        }
        //如果字符串长度为8，每4个为一组，组内两两转换后，组与组之间互换
        if (message.length() == 8) {
            return ByteConvertUtil.parse(message, ByteConvertUtil.INT);
        }

        return 0;

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
