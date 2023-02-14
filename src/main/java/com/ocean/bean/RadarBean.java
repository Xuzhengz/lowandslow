package com.ocean.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * @author 徐正洲
 * @create 2023-01-05 9:55
 * <p>
 * 雷达数据
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RadarBean {
    //主键
    private String id ;
    //报文标识
    private String msgId;
    //报文长度
    private String msgLength;
    //报文帧号
    private String msgFrameNum;
    //报文时间戳
    private Long ts;
    //目标类型
    private String targetType;
    //数据周期
    private String dataPeriod;
    //目标回波幅反
    private String targetEcho;
    //目标距高
    private String targetHeight;
    //目标方位
    private String bearing;
    //目标仰角
    private String elevation;
    //目标x轴距岛
    private String targetXWheelbaseIsland;
    //目标Y轴距高
    private String targetYAxisBaseHeight;
    //目标乙轴距高
    private String targetZAxisBaseHeight;
    //预测目标距高
    private String preHeight;
    //预测目标方位
    private String preBearing;
    //预测目标仰角
    private String preElevation;
    //目标航向
    private String courseOfTheTarget;
    //目标航速
    private String targetSpeed;
    //备用字段1
    private String reserveCol1;
    //备用字段2
    private String reserveCol2;
    //目标x轴速度
    private String targetX_AxisSpeed;
    //目标y轴速度
    private String targetY_AxisSpeed;
    //国际z轴
    private String internationalZ_Axis;
    //目标批号
    private String targetBatchNum;
    //跟踪报文状态
    private String messageStatus;
}
