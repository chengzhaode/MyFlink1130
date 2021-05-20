package com.atguigu.chapter07.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/14 10:30
 */
public class Flink02_State_Keyed_Value {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env
                .socketTextStream("hadoop162",9999)
                .map(line ->{
                    String[] data = line.split(",");
                    return new WaterSensor(
                            data[0],
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2]));
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private ValueState<Integer> lastVcState;
                    // 监控状态由flink的运行时对象管理
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("name", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        Integer currentVc = value.getVc();
                        Integer lastVc = lastVcState.value();
                        if (lastVc != null) {
                        if (currentVc - lastVc > 10) {
                            out.collect(ctx.getCurrentKey() + " 水位上升: " + (currentVc - lastVc) + " 红色预警");
                        }
                        }
                        lastVcState.update(currentVc);
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
