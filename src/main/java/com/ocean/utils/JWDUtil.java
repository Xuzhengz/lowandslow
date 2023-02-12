package com.ocean.utils;

import java.util.ArrayList;
import java.util.List;

public class JWDUtil {
    //XY坐标系转换成经纬度代码
    public static List<Double> XYTOJWD(double X,double Y){
        double L = 6381372 * Math.PI*2;
        double mill = 2.3;
        double JD = (X*1000-(L/2 ))*360/L; // 根据X轴计算经度
        double v = (L / 4 - Y*1000) * mill*2/ (L/2) ;
        double WD = (Math.atan(Math.pow(Math.E, (v / 1.25))) - (0.25 * Math.PI))/0.4 * 180 / Math.PI;//根据Y轴计算纬度
        ArrayList<Double> list = new ArrayList<>();
        list.add(JD);
        list.add(WD);
        return list;
    }

    public static void main(String[] args) {
        System.out.println(XYTOJWD(4, 4));
    }
}