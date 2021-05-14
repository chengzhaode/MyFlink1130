package com.atguigu.chapter05.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
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
public class Flink08_Transform_Reduce {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        DataStreamSource<WaterSensor> s1 = env.fromElements(
                new WaterSensor("sendor_1", 1L, 10),
                new WaterSensor("sendor_2", 2L, 20),
                new WaterSensor("sendor_1", 3L, 30),
                new WaterSensor("sendor_1", 4L, 40),
                new WaterSensor("sendor_1", 5L, 50));
        s1
            .keyBy(ws -> ws.getId())
            .reduce(new ReduceFunction<WaterSensor>() {
                @Override
                public WaterSensor reduce(WaterSensor value1,
                                          WaterSensor value2) throws Exception {
                    value1.setVc(value1.getVc() + value2.getVc());
                    return value1;
                }
            })
            .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
/*
reduce
    之后的流中数据的类型和聚合前必须一致!
    1. 如果一个key只有一个值, 则reduce函数不会执行

 */