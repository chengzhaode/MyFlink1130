package com.atguigu.chapter07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/15 8:37
 */
public class Flink02_Window_Sliding {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env
                .socketTextStream("hadoop162",9998)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(Tuple2.of(word,1L));
                        }
                    }
                })
                .keyBy( t -> t.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(2)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, // 这个是属于那个key
                                        Context ctx,// 上下文: 能够获取窗口的相关信息
                                        Iterable<Tuple2<String, Long>> elements,// 这个窗口内所有的元素
                                        Collector<String> out) throws Exception {
                        ArrayList<Object> words = new ArrayList<>();
                        for (Tuple2<String, Long> t : elements) {
                            words.add(t.f0);
                        }
                        TimeWindow window = ctx.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        out.collect("key=" + key + ", window=[" + start + ", " + end + "), words=" + words);
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
