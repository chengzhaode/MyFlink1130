package com.atguigu.chapter05.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;


/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/13 11:44
 */
public class Flink01_Source_Custom {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        DataStreamSource<WaterSensor> s1 = env.addSource(new MySocketSource("hadoop162",9999));
        s1.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
class MySocketSource implements SourceFunction<WaterSensor> {
    private String host;
    private int port;
    private boolean isCancel = false;

    public MySocketSource(String host, int port) {
        this.host = host;
        this.port = port;
    }
    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {
        Socket socket = new Socket(host, port);
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        String line = reader.readLine();
        // sensor_1,1,10
        while (!isCancel && line != null) {
            String[] data = line.split(",");
            ctx.collect(new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2])));
            line = reader.readLine();  // 阻塞式
        }
    }

    @Override
    public void cancel() {
        isCancel = true;
    }
}