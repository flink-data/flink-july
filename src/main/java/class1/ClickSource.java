package class1;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {

    private Boolean isRunning = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Ken", "Nancy", "Peter", "Jerry"};
        String[] urls = {"fav/", "couponRedemption/", "/cart", "/payment"};
        while (isRunning) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user, url, timestamp));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
