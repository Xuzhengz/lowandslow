package com.ocean.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 徐正洲
 * @create 2023-02-13 15:36
 * <p>
 * 封装目标批号
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TargetType {
    //报文字头
    private String msgHead;
    //目标批号
    private String targetBatchNum;
    //目标类型  0-无人机 1-鸟 2-民航 3-民航 4-不明物体
    private String targetType;
    //目标置信度
    private String  confidence;
    //时间戳
    private Long ts;

}
