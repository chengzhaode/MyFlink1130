/*
package com.atguigu.chapter07.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;


*/
/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/14 10:26
 */


/*
        public class Flink01_State_Operator_BroaderState {
        public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.enableCheckpointing(3000);

        DataStreamSource<String> dataStream = env
                .socketTextStream("hadoop162", 8888);
        DataStreamSource<String> bdStream = env
                .socketTextStream("hadoop162", 9999);
        dataStream
                .connect(bdStream)


    }
}
*/


/*
广播状态的使用:
        需要两个流
        数据流
        正常的数据, 处理业务逻辑, 需要一些状态, 而且每个并行度读取到的状态应该完全一样, 用到广播状态

        广播流
        广播流中的数据, 会被作为广播状态, 广播给数据流中的每个并行度
*/

