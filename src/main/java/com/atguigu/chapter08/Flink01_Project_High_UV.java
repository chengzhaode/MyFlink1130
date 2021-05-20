package com.atguigu.chapter08;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.time.Duration;
import java.util.Date;

/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/15 10:14
 */
public class Flink01_Project_High_UV {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env
                .readTextFile("input/UserBehavior.csv")
                .map(line ->{
                    String[] datas = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(datas[0]),
                            Long.valueOf(datas[1]),
                            Integer.valueOf(datas[2]),
                            datas[3],
                            Long.valueOf(datas[4])
                            );
                })
                .filter(x-> "pv".equals(x.getBehavior()))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((ub, ts) -> ub.getTimestamp() * 1000)
                )
                .keyBy(UserBehavior::getBehavior)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {

                    private MapState<Long, Object> userBehaver;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        userBehaver = getRuntimeContext().getMapState(
                                new MapStateDescriptor<Long, Object>(
                                        "pv",
                                        Long.class,
                                        Object.class));
                    }

                    @Override
                    public void process(String value,
                                        Context context,
                                        Iterable<UserBehavior> elements,
                                        Collector<String> out) throws Exception {
                        userBehaver.clear();
                        for (UserBehavior element : elements) {
                            userBehaver.put(element.getUserId(),null);
                        }
                        long uv = 0L;
                        for (Long key : userBehaver.keys()) {
                            uv++;
                        }
                        String msg = "窗口开始时间：" + new Date(context.window().getStart())
                                + "窗口关闭时间：" + new Date(context.window().getEnd())
                                + "uv: " + uv;
                        out.collect(msg);

                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
