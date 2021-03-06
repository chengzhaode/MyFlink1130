package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/14 19:21
 */
public class Flink01_Project_PV2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env
                .readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(data[0]),
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2]),
                            data[3],
                            Long.valueOf(data[4])
                    );
                })
                .keyBy(UserBehavior -> UserBehavior.getBehavior())
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                    Long sum = 0L;
                    @Override
                    public void processElement(UserBehavior value,
                                               Context ctx,
                                               Collector<Long> out) throws Exception {
                        if("pv".equals(value.getBehavior())){
                            sum++;
                            out.collect(sum);
                        }
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
