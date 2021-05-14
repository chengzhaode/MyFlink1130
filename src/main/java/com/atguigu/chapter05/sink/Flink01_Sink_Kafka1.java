package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;

/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/13 20:47
 */
public class Flink01_Sink_Kafka1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        DataStreamSource<WaterSensor> s1 = env.fromCollection(waterSensors);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers","hadoop162:9092");
        //事务超时时间（严格一次使用的是kafka的事务）
        //kafka服务器默认的是15分钟，flink默认的是1小时
        props.setProperty("transaction.timeout.ms",14*60*1000+"");
        s1
            .addSink(new FlinkKafkaProducer<WaterSensor>(
                    "sensor_2",
                    new KafkaSerializationSchema<WaterSensor>() {
                        @Override
                        public ProducerRecord<byte[], byte[]> serialize(WaterSensor element,
                                                                        @Nullable Long timestamp) {
                            String jsonObj = JSON.toJSONString(element);
                            return new ProducerRecord<>(
                                    "sensor_2",
                                    element.getId().getBytes(),
                                    jsonObj.getBytes()
                            );
                        }
                    },
                    props,
                    FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                    ));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
/*
sink的并行度如果小于kafka的分区数, 则会导致kafka的个分区数据
解决:
    1. 尽量让你sink的并行度和要写入的Kafka的topic的分区数保持一致!
    2. 写的时候最好使用轮询的方式, 或者指定分区的索引

 */