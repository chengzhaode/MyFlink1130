package com.atguigu.chapter05.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/13 11:44
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        List<Integer> list = Arrays.asList(1, 20, 2, 30, 40);
        DataStreamSource<Integer> s1 = env.fromCollection(list);
        s1.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
