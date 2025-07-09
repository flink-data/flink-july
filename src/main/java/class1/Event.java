package class1;

public class Event {

    public String url;
    public String user;
    public Long timestamp;


    public Event(String user, String url, Long timestamp) {
        this.url = url;
        this.user = user;
        this.timestamp = timestamp;
    }

    public Event() {
    }

    public String toString() {
        return "Event{" +
                "url='" + url + '\'' +
                ", user='" + user + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
