package class4;

public class ItemViewCount {

    public String ItemId;
    public Long windowEnd;
    public Long count;

    public ItemViewCount() {
    }

    public ItemViewCount(String itemId, Long windowEnd, Long count) {
        ItemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "ItemId='" + ItemId + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
