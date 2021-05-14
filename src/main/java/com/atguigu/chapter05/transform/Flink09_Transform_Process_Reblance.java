package com.atguigu.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/13 18:35
 */
public class Flink09_Transform_Process_Reblance {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4);
        s1
            .keyBy(t -> t % 2)
            .map(x -> x)
            .rebalance()
            //.rescale()
            .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
