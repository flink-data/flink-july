package class2;

import class1.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

public class SinkToFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);
        DataStream<Event> stream  = environment.fromElements(
                new Event("Tom", "/cart", 1000L),
                new Event("Mary", "/home", 2000L),
                new Event("Ben", "/fav", 3000L),
                new Event("Jerry", "/cart", 4000L),
                new Event("Tom", "/fav", 5000L),
                new Event("Kate", "/cart", 6000L),
                new Event("Karen", "/fav", 7000L),
                new Event("Roy", "/order", 8000L),
                new Event("Drew", "/refund", 9000L)
        );
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(
                new Path("C:\\Users\\admin\\IdeaProjects\\flink-july\\src\\main\\resources"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withMaxPartSize(1024 * 1024 * 1024).build()
                ).build();
        stream.map(data -> data.toString()).addSink(streamingFileSink);
        environment.execute();
    }
}
