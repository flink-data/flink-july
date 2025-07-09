package class2;

import class1.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichMapFunctionTest {

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

        streamSource.map(new RichMapFunction<Event, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("Start of the life cycle: " + getRuntimeContext().getIndexOfThisSubtask());
            }

            @Override
            public Integer map(Event value) throws Exception {
                return value.url.length();
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("End of the life cycle: " + getRuntimeContext().getIndexOfThisSubtask());
            }
        }).print();

        environment.execute();
    }
}
