import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * TinyFlink - 一个极简的“流处理引擎”示例
 *
 * 功能特点：
 * - 支持链式 API: fromElements(...).map(...).filter(...).print().execute()
 * - Operator chain：每个算子处理完数据后直接同步推送给下游
 * - Source 支持两种执行模式：同步批量推送、异步定时推送（模拟流式数据）
 *
 * 目标：
 *   用最小实现演示 Flink 中核心概念：
 *   - StreamRecord（流事件封装，包含 value + timestamp）
 *   - Operator（算子接口）
 *   - OperatorChain（算子链）
 *   - Source → Transformation → Sink 的完整执行流程
 */
public class TinyFlink {

    /* --------------------------
     * 核心数据结构和函数接口
     * -------------------------- */

    // StreamRecord：对流中的一条数据进行封装，带上时间戳
    public static class StreamRecord<T> {
        public final T value;
        public final long timestamp;
        public StreamRecord(T value) {
            this(value, System.currentTimeMillis()); // 默认用当前时间戳
        }
        public StreamRecord(T value, long ts) {
            this.value = value;
            this.timestamp = ts;
        }
        public String toString(){
            return "StreamRecord(" + value + ", ts=" + timestamp + ")";
        }
    }

    // Map 函数接口：接收一个输入值，返回一个输出值
    public static interface MapFunction<IN, OUT> {
        OUT map(IN value) throws Exception;
    }

    // Filter 函数接口：接收一个输入值，返回是否保留
    public static interface FilterFunction<T> {
        boolean filter(T value) throws Exception;
    }

    // 抽象算子：所有算子都继承自它
    public static abstract class Operator {
        // 下游消费者（Consumer 接口），接收上游处理后的 StreamRecord
        protected Consumer<StreamRecord<?>> downstream;

        public void setDownstream(Consumer<StreamRecord<?>> downstream) {
            this.downstream = downstream;
        }

        // 处理来自上游的数据
        public abstract void process(StreamRecord<?> record) throws Exception;
    }

    /* --------------------------
     * 算子实现：Map / Filter / Sink
     * -------------------------- */

    // Map 算子：对输入值做转换
    public static class MapOperator<IN, OUT> extends Operator {
        private final MapFunction<IN, OUT> func;

        public MapOperator(MapFunction<IN, OUT> f) {
            this.func = f;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void process(StreamRecord<?> record) throws Exception {
            // 强转为输入类型
            IN in = (IN) record.value;
            // 调用用户函数 map
            OUT out = func.map(in);
            // 封装为新的 StreamRecord 并推送下游
            if (downstream != null) {
                downstream.accept(new StreamRecord<>(out, record.timestamp));
            }
        }
    }

    // Filter 算子：根据条件保留或丢弃数据
    public static class FilterOperator<T> extends Operator {
        private final FilterFunction<T> func;

        public FilterOperator(FilterFunction<T> f) { this.func = f; }

        @SuppressWarnings("unchecked")
        @Override
        public void process(StreamRecord<?> record) throws Exception {
            T in = (T) record.value;
            // 通过条件则继续传递
            if (func.filter(in)) {
                if (downstream != null) downstream.accept(record);
            }
        }
    }

    // Sink：打印输出（终点）
    public static class PrintSink extends Operator {
        @Override
        public void process(StreamRecord<?> record) {
            System.out.println(record);
        }
    }

    /* --------------------------
     * Stream API（构建算子链）
     * -------------------------- */

    public static class Stream<T> {
        // head：第一个算子（source 会推数据进 head）
        private Operator head;
        // tail：链的尾部（便于新增算子时接上）
        private Operator tail;

        // 存放 source 的初始元素（同步或异步执行时用）
        private List<T> sourceElements;

        private Stream() {}

        // 创建一个流（Source）——直接从一组元素构建
        @SafeVarargs
        public static <T> Stream<T> fromElements(T... elements) {
            Stream<T> s = new Stream<>();
            s.sourceElements = Arrays.asList(elements);
            return s;
        }

        // 添加 map 算子，并返回新的 Stream（泛型类型切换）
        public <R> Stream<R> map(MapFunction<T, R> mapFunc) {
            MapOperator<T, R> mapOp = new MapOperator<>(mapFunc);
            attachOperator(mapOp);
            Stream<R> next = new Stream<>();
            next.head = this.head;         // 继承原链 head
            next.tail = mapOp;             // 新的链尾
            next.sourceElements = (List<R>) this.sourceElements; // 共用 source
            return next;
        }

        // 添加 filter 算子
        public Stream<T> filter(FilterFunction<T> filterFunc) {
            FilterOperator<T> filt = new FilterOperator<>(filterFunc);
            attachOperator(filt);
            this.tail = filt;
            return this;
        }

        // 添加 sink：print
        public Stream<T> print() {
            PrintSink sink = new PrintSink();
            attachOperator(sink);
            this.tail = sink;
            return this;
        }

        // 将算子接到链尾
        private void attachOperator(Operator op) {
            if (head == null) {
                // 首次添加算子：直接作为 head & tail
                head = op;
                tail = op;
            } else {
                // 把上一个 tail 的 downstream 设置为调用下一个算子
                Operator prev = tail;
                prev.setDownstream(rec -> {
                    try {
                        op.process(rec);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                // 更新 tail
                tail = op;
            }
        }

        // 同步执行：一次性推送所有 source 元素
        public void execute() {
            if (head == null) {
                System.out.println("No operators attached. Nothing to execute.");
                return;
            }
            for (T e : sourceElements) {
                StreamRecord<T> rec = new StreamRecord<>(e);
                try {
                    head.process(rec);
                } catch (Exception ex) {
                    throw new RuntimeException("Error processing record: " + rec, ex);
                }
            }
        }

        // 异步执行：定时推送 source 元素，模拟“流”
        public void executeAsync(long intervalMillis) {
            if (head == null) {
                System.out.println("No operators attached. Nothing to execute.");
                return;
            }
            ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
            final Iterator<T> it = sourceElements.iterator();
            // 每隔 intervalMillis 发送一个元素
            exec.scheduleAtFixedRate(() -> {
                if (!it.hasNext()) {
                    exec.shutdown();
                    return;
                }
                T e = it.next();
                StreamRecord<T> rec = new StreamRecord<>(e);
                try {
                    head.process(rec);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }, 0, intervalMillis, TimeUnit.MILLISECONDS);
            // 简单等待任务结束（防止主线程退出）
            try {
                exec.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /* --------------------------
     * Main: 示例 pipeline
     * -------------------------- */

    public static void main(String[] args) {
        // Example 1: 同步执行
        System.out.println("== Sync execution ==");
        Stream.fromElements(1,2,3,4,5)
                .map((Integer x) -> x * 2)         // map: 把每个数字 *2
                .filter((Integer x) -> x % 4 == 0) // filter: 只保留能被4整除的
                .print()                           // sink: 打印输出
                .execute();

        // Example 2: 异步执行（每隔 200ms 发送一条数据）
        System.out.println("\n== Async (interval) execution ==");
        Stream.fromElements("a","bb","ccc","dddd")
                .map((String s) -> s + "[" + s.length() + "]") // 拼接字符串 + 长度
                .print()                                       // sink: 打印
                .executeAsync(200);
    }
}
