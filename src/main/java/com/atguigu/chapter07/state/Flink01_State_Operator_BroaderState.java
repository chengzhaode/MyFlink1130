package com.atguigu.chapter07.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/13 15:56
 */

        public class Flink01_State_Operator_BroaderState {
        public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.enableCheckpointing(3000);
            MapStateDescriptor<String, String> bdStateDescriptor = new MapStateDescriptor<>("bdState", String.class, String.class);

            DataStreamSource<String> dataStream = env
                .socketTextStream("hadoop162", 8888);
            BroadcastStream<String> bdStream = env
                    .socketTextStream("hadoop162", 9999)
                    .broadcast(bdStateDescriptor);
            dataStream
                    .connect(bdStream)
                    .process(new BroadcastProcessFunction<String, String, String>() {
                        //数据流的数据会走这里
                        // 获取广播状态
                        @Override
                        public void processElement(String value,
                                                   ReadOnlyContext ctx,
                                                   Collector<String> out) throws Exception {
                            ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(bdStateDescriptor);
                            if ("0".equals(broadcastState)) {
                                out.collect("切换第0状态");
                            }else if ("1".equals(broadcastState)) {
                                out.collect("切换第1状态");
                            }else {
                                out.collect("切换其他状态");
                            }
                        }
                        // 广播流的数据会走这里
                        // 把广播流的数据, 我会放入到广播状态中
                        @Override
                        public void processBroadcastElement(String value,
                                                            Context ctx,
                                                            Collector<String> out) throws Exception {
                            BroadcastState<String, String> broadcastState = ctx.getBroadcastState(bdStateDescriptor);
                            broadcastState.put("switch",value);
                        }
                    })
                    .print();
            env.execute();

        }
}


/*广播状态的使用:
        需要两个流
        数据流
        正常的数据, 处理业务逻辑, 需要一些状态, 而且每个并行度读取到的状态应该完全一样, 用到广播状态

        广播流
        广播流中的数据, 会被作为广播状态, 广播给数据流中的每个并行度*/

