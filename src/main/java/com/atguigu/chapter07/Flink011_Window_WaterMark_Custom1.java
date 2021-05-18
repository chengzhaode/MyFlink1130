package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.annotate.JsonUnwrapped;

import java.time.Duration;


/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/12 16:39
 */
public class Flink011_Window_WaterMark_Custom1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(
                    data[0],
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2]));

            })
            .assignTimestampsAndWatermarks(
                    new WatermarkStrategy<WaterSensor>() {
                        @Override
                        public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                            return null;
                        }
                    })
            .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<WaterSensor> elements,
                                        Collector<String> out) throws Exception {
                        int count = 0;
                        for (WaterSensor ws : elements) {
                            count++;
                        }
                        TimeWindow w = ctx.window();
                        out.collect(
                                "当前Key=" + key
                                        + "窗口: [ " + w.getStart() / 1000 + "," + w.getEnd() / 1000 + " ), "
                                        + "元素个数: " + count
                        );
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public class MyPeriodWaterMark implements WatermarkGenerator<WaterSensor>{

        long maxTs = Long.MIN_VALUE + 3000 + 1;
        @Override
        public void onEvent(WaterSensor event,
                            long eventTimestamp,
                            WatermarkOutput output) {
            System.out.println("MyPeriodWaterMark.onEvent");
            maxTs = Math.max(eventTimestamp,maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("MyPeriodWaterMark.onPeriodicEmit");
            output.emitWatermark(new Watermark(maxTs -3000-1));
        }
    }
}
