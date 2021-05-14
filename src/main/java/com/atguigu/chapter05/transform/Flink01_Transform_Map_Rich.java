package com.atguigu.chapter05.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/13 18:35
 */
public class Flink01_Transform_Map_Rich {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4);
        s1
                .map(new RichMapFunction<Integer, Integer>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //做初始化操作，连接数据库,执行次数与并行度一致
                        System.out.println("open。。。");
                    }

                    @Override
                    public void close() throws Exception {
                        //程序关闭时执行,执行次数与并行度一致
                        System.out.println("close");
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value * value;
                    }
                })
                .map(value -> value * value)
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
