package com.atguigu.chapter05.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/13 18:35
 */
public class Flink01_Transform_FlatMap {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4);
        s1
                .flatMap(new FlatMapFunction<Integer, Integer>() {
                    @Override
                    public void flatMap(Integer value,
                                        Collector<Integer> out) throws Exception {
                        out.collect(value * value);
                        out.collect(value * value * value);
                    }
                })
                .print();
        env.execute();
    }
}
