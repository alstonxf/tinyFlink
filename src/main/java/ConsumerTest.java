import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class ConsumerTest {

    public class Consumer1 implements Consumer<String> {
        List<String> list = new ArrayList<>();
        @Override
        public void accept(String s) {
            list.add(s);
        }

        public ArrayList<String> accept2(String s) {
            list.add(s);
            return (ArrayList<String>) list;
        }

        @Override
        public Consumer<String> andThen(Consumer<? super String> after) {
            return Consumer.super.andThen(after);
        }
    }

    @Test
    public void testAccept() {
        List<String> list = new ArrayList<>();

        // 定义一个 Consumer，把输入加到 list 里

//        Consumer<String> addToList = s -> list.add(s);
        Consumer1 addToList = new Consumer1();
        list = addToList.accept2("hello");
        list = addToList.accept2("world");
        System.out.println(list);

        assertEquals(2, list.size());
        assertEquals("hello", list.get(0));
        assertEquals("world", list.get(1));
    }

    public class Consumer2<String> implements Consumer{
        List<String> results;
        String name;

        public Consumer2(List<String>l,String name){
            this.results = l;
            this.name = name;
            System.out.println("Consumer2实例化，参数为"+l+name);
        }

        @Override
        public void accept(Object s) {
            results.add((String) (name + s.toString()));
            System.out.println((String) (name + s.toString()));
        }

        @Override
        public Consumer andThen(Consumer after) {
            return Consumer.super.andThen(after);
        }
    }

    @Test
    public void testAndThen1() {
        Consumer2<String> first = new Consumer2<String>(new ArrayList<>(),"first");
        first.accept("hello1");
        Consumer2<String> second = new Consumer2<String>(new ArrayList<>(),"second");
        second.accept("second2");
        //    default Consumer<T> andThen(Consumer<? super T> after) {
        //        Objects.requireNonNull(after);
        //        return (T t) -> { accept(t); after.accept(t); };
        //    }
        // 返回一个新的Consumer，组合：这个Consumer的accept会先执行 first.accept，再执行 second.accept
        // 相当于
        //return new Consumer<T>() {
        //    @Override
        //    public void accept(T t) {
        //        Consumer.this.accept(t); // 调用当前对象的 accept
        //        after.accept(t);         // 调用传入的 after 的 accept
        //    }
        //};
        Consumer combined = first.andThen(second);
        System.out.println("-------------");
        combined.accept("11abc");

//        assertEquals(2, results.size());
//        assertEquals("first:abc", results.get(0));
//        assertEquals("second:abc", results.get(1));
    }

    @Test
    public void testAndThen() {
        List<String> results = new ArrayList<>();

        Consumer<String> first = s -> results.add("first:" + s);
        Consumer<String> second = s -> results.add("second:" + s);

        // 组合：先执行 first.accept，再执行 second.accept
        Consumer<String> combined = first.andThen(second);

        combined.accept("abc");

        assertEquals(2, results.size());
        assertEquals("first:abc", results.get(0));
        assertEquals("second:abc", results.get(1));
    }

    @Test(expected = NullPointerException.class)
    public void testAndThenNullThrows() {
        Consumer<String> consumer = s -> System.out.println(s);

        // 传入 null 的时候应该抛 NullPointerException
        consumer.andThen(null);
    }
}
