package com.ocean.common;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author 徐正洲
 * @create 2023-02-13 10:27
 */
public class DxmConfig {
    private static ParameterTool parameterTool;

    static {
        try {
            InputStream resourceAsStream = DxmConfig.class.getClassLoader().getResourceAsStream("application.properties");
            parameterTool = ParameterTool.fromPropertiesFile(resourceAsStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * mysql配置
     */
    // Mysql 驱动
    public static final String MYSQL_DRIVER = parameterTool.get("mysql_driver");
    // Mysql 连接 URL
    public static final String MYSQL_URL = parameterTool.get("mysql_url");
    // Mysql 账号
    public static final String MYSQL_USERNAME = parameterTool.get("mysql_username");
    // Mysql 密码
    public static final String MYSQL_PASSWORD = parameterTool.get("mysql_password");
    // Mysql 每批数据量
    public static final int MYSQL_BATCH_SIZE = Integer.parseInt(parameterTool.get("mysql_batch_size"));
    // Mysql 发送间隔毫秒
    public static final Long MYSQL_BATCH_interval_MS = Long.parseLong(parameterTool.get("mysql_batch_interval_ms"));

    /**
     * kakfa配置
     */
    public static final String KAFKA_SERVER = parameterTool.get("kafka_server");
    public static final String KAFKA_ISOLATION_LEVEL = parameterTool.get("kafka_isolation_level");



}
