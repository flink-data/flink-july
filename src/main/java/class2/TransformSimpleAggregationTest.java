package class2;

import class1.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggregationTest {

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
        streamSource.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        }).max("timestamp").print();
        //MaxBy: > Event{url='/cart', user='Drew', timestamp=7000}
        //MaxBy: > Event{url='/cart', user='Drew', timestamp=8000}
        //MaxBy: > Event{url='/cart', user='Drew', timestamp=9000}
        streamSource.filter(data -> data.user.equals("Drew")).keyBy(data -> data.user).maxBy("timestamp").print("MaxBy: ");
        //MaxBy: > Event{url='/cart', user='Drew', timestamp=7000}
        //MaxBy: > Event{url='/fav', user='Drew', timestamp=8000}
        //MaxBy: > Event{url='/refund', user='Drew', timestamp=9000}
        environment.execute();
    }
}
