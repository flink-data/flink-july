package class2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Integer> streamSource1 = environment.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Long> streamSource2 = environment.fromElements(6L, 7L, 8L, 9L);
        streamSource1.connect(streamSource2).map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "streamSource1: " + value.toString();
            }

            @Override
            public String map2(Long value) throws Exception {
                return "streamSource2: " + value.toString();
            }
        }).print();
        environment.execute();
    }
}
