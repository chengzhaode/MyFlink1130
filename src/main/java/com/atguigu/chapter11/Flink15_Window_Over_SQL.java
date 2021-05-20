package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.UNBOUNDED_ROW;


/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/19 18:41
 */

public class Flink15_Window_Over_SQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "create table sensor( "+
                "id string, "+
                "ts string, "+
                "vc int, "+
                "et as to_timestamp(from_unixtime(ts) ), "+
                "watermark for et as et - interval '4' second "+
                " )with("+
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/sensor.txt', " +
                " 'format' = 'csv' " +
                ")");
        tEnv.sqlQuery("select"+
                        " id, ts,vc, "+
                        "sum(vc) over(partition by id order et rows between unbounded preceding and current row) vc_sum "+
                        "from sensor")
                .execute()
                .print();

    }
}