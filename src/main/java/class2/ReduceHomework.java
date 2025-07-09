package class2;

import class1.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceHomework {

    // 1. Each User's unique URL,  First step reduce
    // 2. Global user with max number unique url visited, Second step reduce
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> streamSource = environment.fromElements(
                new Event("Tom", "/cart", 1000L),
                new Event("Mary", "/home", 2000L),
                new Event("Ben", "/fav", 3000L),
                new Event("Jerry", "/cart", 9000L),
                new Event("Tom", "/cart", 8000L),
                new Event("Drew", "/cart", 7000L),
                new Event("Drew", "/fav", 8000L),
                new Event("Roy", "/order", 18000L),
                new Event("Drew", "/refund", 9000L)
        );

        //user, UrlSet
        SingleOutputStreamOperator<UserVisitSite> userUrlSet = streamSource.map(new MapFunction<Event, UserVisitSite>() {
            @Override
            public UserVisitSite map(Event value) throws Exception {
                //TODO
                return null;
            }
        });

        // add all Urls together
        SingleOutputStreamOperator<UserVisitSite> urlCountPerUSer = userUrlSet.keyBy(value -> value.user).reduce(new ReduceFunction<UserVisitSite>() {
            @Override
            public UserVisitSite reduce(UserVisitSite value1, UserVisitSite value2) throws Exception {
                //TODO
                return null;
            }
        });

        SingleOutputStreamOperator<UserVisitSite> topCount = urlCountPerUSer.keyBy(value -> "global")
                .reduce(new ReduceFunction<UserVisitSite>() {
                    @Override
                    public UserVisitSite reduce(UserVisitSite value1, UserVisitSite value2) throws Exception {
                        return null;
                    }
                });
        urlCountPerUSer.print();
        topCount.print();
        environment.execute();
    }
}
