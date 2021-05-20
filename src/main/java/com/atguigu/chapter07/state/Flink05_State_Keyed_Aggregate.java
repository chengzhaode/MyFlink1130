package com.atguigu.chapter07.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/13 15:56
 */
public class Flink05_State_Keyed_Aggregate {
    public static void main(String[] args) {
        // 把每个单词存入到我们的列表状态
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
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                private AggregatingState<Integer, Double> aggState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    aggState = getRuntimeContext().getAggregatingState(
                            new AggregatingStateDescriptor<>(
                                    "AggState",
                                    new AggregateFunction<Integer, Tuple2<Integer, Long>, Double>() {

                                        @Override
                                        public Tuple2<Integer, Long> createAccumulator() {
                                            return Tuple2.of(0, 0L);
                                        }

                                        @Override
                                        public Tuple2<Integer, Long> add(Integer value, Tuple2<Integer, Long> accumulator) {
                                            return Tuple2.of(accumulator.f0, accumulator.f1 + value);
                                        }

                                        @Override
                                        public Double getResult(Tuple2<Integer, Long> accumulator) {
                                            return accumulator.f0 * 1.0 / accumulator.f1;
                                        }

                                        @Override
                                        public Tuple2<Integer, Long> merge(Tuple2<Integer, Long> a, Tuple2<Integer, Long> b) {
                                            return null;
                                        }
                                    },
                                    Types.TUPLE(Types.INT, Types.LONG)

                            ));
                }

                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    Integer vc = value.getVc();
                    aggState.add(vc);
                    out.collect(ctx.getCurrentKey() + ":" +aggState.get());

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
