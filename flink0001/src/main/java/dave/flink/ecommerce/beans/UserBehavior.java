package dave.flink.ecommerce.beans;

import dave.flink.ecommerce.HotItems;
import scala.concurrent.java8.FuturesConvertersImpl;

import java.util.*;

public class UserBehavior {

    private long userId;
    private long itemId;
    private int categoryId;
    private String behavior;
    private long timestamp;

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public UserBehavior(long userId, long itemId, int categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public static void main(String[] args)  throws Exception {

        Random ran2 = new Random(10);
        List<ItemViewCount> allItems = new ArrayList<ItemViewCount>();
        for (int i = 0; i < 5; i++) {
            ItemViewCount ivc = new ItemViewCount(Long.valueOf(i), Long.valueOf(i), ran2.nextInt());
            allItems.add(ivc);
        }
//        allItems.sort(new IVCComparator());
        Collections.sort(allItems, new IVCComparator());
        for (int i = 0; i < 5; i++) {
            System.out.println(allItems.get(i));
        }

        ArrayList list = new ArrayList();
        list.add(new Person("lcl", 28));
        list.add(new Person("fx", 23));
        list.add(new Person("wqx", 29));
        Comparator comp = new Mycomparator();
        Collections.sort(list, comp);
        for (int i = 0; i < list.size(); i++) {
            Person p = (Person) list.get(i);
            System.out.println(p.getName());
        }


    }

    public static class IVCComparator implements Comparator<ItemViewCount> {

        @Override
        public int compare(ItemViewCount o1, ItemViewCount o2) {
            if (o1.getCount() < o2.getCount())
                return -1;
            else
                return 0;
        }
    }

    public static class Person {
        String name;
        int age;

        public Person(String name, int age) {
            this.name = name;
            this.age = age;

        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

    }

    public static class Mycomparator implements Comparator {

        public int compare(Object o1, Object o2) {
            Person p1 = (Person) o1;
            Person p2 = (Person) o2;
            if (p1.age < p2.age) return 1;
            else return 0;
        }

    }


}
