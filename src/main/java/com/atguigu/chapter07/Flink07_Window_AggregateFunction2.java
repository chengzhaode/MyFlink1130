package com.atguigu.chapter07;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import static sun.misc.Version.print;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/12 11:29
 */
public class Flink07_Window_AggregateFunction2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env
            .socketTextStream("hadoop162", 9999)
            .map(new MapFunction<String,Tuple2<String,Long>>() {

                @Override
                public Tuple2<String, Long> map(String value) throws Exception {
                    String[] word = value.split(",");
                    return Tuple2.of(word[0],Long.valueOf(word[1]));
                }
            })
            .keyBy(t -> t.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .aggregate(
                    new AggregateFunction<Tuple2<String, Long>, Tuple2<Long,Integer>, Double>() {


                        @Override
                        public Tuple2<Long, Integer> createAccumulator() {
                            return Tuple2.of(0L,0);
                        }

                        @Override
                        public Tuple2<Long, Integer> add(
                                Tuple2<String, Long> value,
                                Tuple2<Long, Integer> accumulator) {
                            return Tuple2.of(accumulator.f0+value.f1,accumulator.f1+1);
                        }

                        @Override
                        public Double getResult(Tuple2<Long, Integer> accumulator) {
                            return accumulator.f0 * 1.0/accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                            return null;
                        }
                    }
                   /* ,
                    new ProcessWindowFunction<Double, Double, String, TimeWindow>() {
                        @Override
                        public void process(String Key,
                                            Context context,
                                            Iterable<Double> elements,
                                            Collector<Double> out) throws Exception {

                        }
                    }*/
            )
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
