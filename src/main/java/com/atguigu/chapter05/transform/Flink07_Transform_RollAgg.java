package com.atguigu.chapter05.transform;

import com.atguigu.bean.WaterSensor;
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
public class Flink07_Transform_RollAgg {
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
        s1.keyBy(WaterSensor::getId)
        //.sum("vc")
        //.max("vc")
        //.min("vc")
        .maxBy("vc",true)
        .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
1. 使用sum max min的必须保证字段的值是数字类型
2. 在聚合的时候, 除了分组字段和聚合字段, 默认情况下其他的字段的值和第一个元素保持一致

3. maxBy(, boolean)
    boolean如果是true, 则当两个值相等时, 其他的字段选择第一个值
    boolean如果是false, 则当两个值相等时, 其他的字段选择最新的那个

    如果值不等, 其他的字段选择的是随着最大或者最小来选
 */
