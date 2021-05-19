package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.CURRENT_ROW;

/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/18 10:59
 */
public class Flink01_Table_BaseUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorDataStream = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));
        // 1. 先有table的执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2. 把流转成动态表
        Table table = tEnv.fromDataStream(waterSensorDataStream);
        // 3. 在动态表上执行连续查询
        Table result = table.where($("id").isEqual("sensor_1"))
                .select($("id").as("idd"), $("ts"));
        result.execute().print();
//        // 4. 把连续查询的结果(动态表) 转成流
//        DataStream<Row> rowDataStream = tEnv.toAppendStream(result, Row.class);
//        // 5. 把流sink出去
//        rowDataStream.print();
//        env.execute();
    }
}
