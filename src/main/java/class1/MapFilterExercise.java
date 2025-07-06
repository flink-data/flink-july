package class1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/* Map and Filter
接受一批用户，有名字和年龄两项，年龄可能为空
先使用map算子把名字换成全部大写字母，如果年龄为空的话改为0岁，
输出元素新增一个字段tag，如果年龄大于等于18，tag = adult，小于则等于 minor
使用filter算子，筛选出所有 tag == adult的用户
*  Input: {name: Tom, Age: 17}
*         {name: Nancy, Age: 25}
*         {name: Jane, Age: null}
*  Output:
*         {name: NANCY, Age: 25, tag: adult}
* */
public class MapFilterExercise {
    public static class User{
        String name;
        Integer age;

        public User(String name, Integer age) {
            this.name = name;
            this.age = age;
        }
    }
    public static class DetailUser{

    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<User> stream = env.fromElements(
                new User("Peter", 30),
                new User("Nancy", 25),
                new User("Jane", null)
        );
        DataStream<DetailUser> users = stream.map(new MapFunction<User, DetailUser>() {
            @Override
            public DetailUser map(User value) throws Exception {
                return null;
            }
        });
        DataStream<DetailUser> filterdUsers = users.filter(new FilterFunction<DetailUser>() {
            @Override
            public boolean filter(DetailUser value) throws Exception {
                return false;
            }
        });
        filterdUsers.print();
        env.execute();
    }
}
