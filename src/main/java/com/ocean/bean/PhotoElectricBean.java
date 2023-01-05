package com.ocean.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 徐正洲
 * @create 2023-01-05 9:57
 *
 * 光电数据
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PhotoElectricBean {
    //TODO 目标距离
    private String distance;
    //TODO 目标方位
    private String bearing;
    //TODO 目标仰角
    private String Elevation;
    //TODO 预测目标距离
    private String preDistance;
    //TODO 预测目标方位
    private String preBearing;
    //TODO 预测目标仰角
    private String preElevation;
    //TODO 报文状态
    private String messageStatus;
    //TODO 目标类型
    private String targetType;
    //TODO 时间戳
    private String ts;
    //TODO 来源类型
    private String sourceType;
}
