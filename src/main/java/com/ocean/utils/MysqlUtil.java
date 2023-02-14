package com.ocean.utils;

import com.ocean.common.DxmConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;

/**
 * @author 徐正洲
 * @date 2022/11/20-19:37
 */
public class MysqlUtil {
    public static <T> SinkFunction<T> getMysqlSink(String sql) {

        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) {
                        Class<?> tClass = t.getClass();
                        Field[] declaredFields = tClass.getDeclaredFields();
                        //定义偏移量
                        int offset = 0;

                        for (int i = 0; i < declaredFields.length; i++) {
                            Field field = declaredFields[i];
                            field.setAccessible(true);
                            Object value = field.get(t);
                            preparedStatement.setObject(i + 1 - offset, value);
                        }
                    }
                },
                JdbcExecutionOptions.builder()
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(DxmConfig.MYSQL_DRIVER)
                        .withUrl(DxmConfig.MYSQL_URL)
                        .withUsername(DxmConfig.MYSQL_USERNAME)
                        .withPassword(DxmConfig.MYSQL_PASSWORD)
                        .build()
        );

    }
}