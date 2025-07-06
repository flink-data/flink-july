package class1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FromClass {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Person> stream = environment.fromElements(
                new Person("Peter", 30),
                new Person("Nancy", 2),
                new Person("Ken", 20)
        );
        DataStream<Person> adults = stream.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person value) throws Exception {
                return value.name.equals("Nancy");
            }
        });
        adults.print();
        environment.execute();
    }
    public static class Person {
        public String name;
        public Integer age;
        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
        public Person() {
        }
        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }
    }
}
