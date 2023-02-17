package com.ocean.utils;

import cn.hutool.core.util.ByteUtil;
import cn.hutool.core.util.HexUtil;

import java.nio.ByteOrder;

/**
 * pom.xml  需要依赖
 * <dependency>
 *             <groupId>cn.hutool</groupId>
 *             <artifactId>hutool-all</artifactId>
 *             <version>5.8.12</version>
 * </dependency>
 */
public class ByteConvertUtil {
    /**
     *  2字节类型
     */
    public static final String SHORT = "SHORT";
    /**
     * 4字节类型
     */
    public static final String INT = "INT";

    /**
     * 使用案例
     * @param args
     */
    public static void main(String[] args) {
        System.out.println( parse("c6c6",SHORT));
        System.out.println( parse("e2060000",INT));
    }
    public static int parse(String hex,String type){
        if (SHORT.equals(type)){
            return ByteUtil.bytesToShort(HexUtil.decodeHex(hex), ByteOrder.LITTLE_ENDIAN);
        }else {
            return ByteUtil.bytesToInt(HexUtil.decodeHex(hex), ByteOrder.LITTLE_ENDIAN);
        }
    }
}
