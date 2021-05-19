package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/18 10:59
 */
public class Flink02_Table_BaseUse_Agg {
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
        // select id, sum(vc) from t where vc>=10 group by id
        Table result = table
                .where($("vc").isGreaterOrEqual(10))
                .groupBy($("id"))
                .select($("id"), $("vc").sum().as("vc_sum"));

        result.execute().print();
    }
}
