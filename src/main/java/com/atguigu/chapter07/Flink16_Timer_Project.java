package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/12 11:29
 */
public class Flink16_Timer_Project {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env
            .socketTextStream("hadoop162", 9999)
            .map(value -> {
                String[] datas = value.split(",");
                return new WaterSensor(
                    datas[0],
                    Long.valueOf(datas[1]),
                    Integer.valueOf(datas[2]));
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(((element, recordTimestamp) -> element.getTs()*1000))
            )
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                private double lastVc;
                long timeTs;
                Boolean isFirst = true;

                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                if(isFirst) {
                    System.out.println("第一条...");
                    isFirst = false;
                    timeTs = ctx.timestamp() + 5000;
                    ctx.timerService().registerEventTimeTimer(timeTs);
                }
                else{
                    if(value.getVc() <= lastVc){
                        System.out.println("水位没有上升");
                        // 出现水位没有上升:    取消定时器
                        // 1. 上次注册的定时器取消
                        ctx.timerService().deleteEventTimeTimer(timeTs);
                        // 2. 重新注册一个新的定时器
                        timeTs = ctx.timestamp() + 5000;
                        ctx.timerService().registerEventTimeTimer(timeTs);
                    }else {
                        System.out.println("水位上升");
                    }
                }
                    lastVc = value.getVc();
                }

                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    out.collect("传感器: " + ctx.getCurrentKey() + " 连续5s水位上升, 红色预警");
                    isFirst = true;
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
