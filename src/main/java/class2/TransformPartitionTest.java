package class2;

import class1.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformPartitionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> streamSource = environment.fromElements(
                new Event("Tom", "/cart", 1000L),
                new Event("Mary", "/home", 2000L),
                new Event("Ben", "/fav", 3000L),
                new Event("Jerry", "/cart", 9000L),
                new Event("Tom", "/fav", 8000L),
                new Event("Drew", "/cart", 7000L),
                new Event("Drew", "/fav", 8000L),
                new Event("Roy", "/order", 18000L),
                new Event("Drew", "/refund", 9000L)
        );

        streamSource.shuffle().print().setParallelism(4);
        streamSource.rebalance().print().setParallelism(4);
        environment.execute();
    }
}
