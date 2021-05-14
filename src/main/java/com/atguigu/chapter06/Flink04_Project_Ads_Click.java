package com.atguigu.chapter06;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/14 20:57
 */
//private Long userId;
//    private Long adId;
//    private String province;
//    private String city;
//    private Long timestamp;
//      429984,2244074,guangdong,shenzhen,1511658600
    //需求：不同省份用户对不同广告的点击量
public class Flink04_Project_Ads_Click {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env
                .readTextFile("input/AdClickLog.csv")
                .map(new MapFunction<String, AdsClickLog>() {
                    @Override
                    public AdsClickLog map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new AdsClickLog(
                                Long.valueOf(data[0]),
                                Long.valueOf(data[1]),
                                data[2],
                                data[3],
                                Long.valueOf(data[4]));
                    }
                })
                .map(new MapFunction<AdsClickLog, Tuple2<Tuple2<String,Long>,Long>>() {
                    @Override
                    public Tuple2<Tuple2<String, Long>, Long> map(AdsClickLog value) throws Exception {

                        return Tuple2.of(Tuple2.of(value.getProvince(),value.getAdId()),1L);
                    }
                })
                .keyBy(new KeySelector<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
