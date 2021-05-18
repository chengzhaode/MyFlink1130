package com.atguigu.chapter08;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/15 10:14
 */
public class Flink01_Project_High_UV {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env
                .readTextFile("");

    }
}
