import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * MiniFlink - 一个极简的“流处理引擎”示例
 *
 * Features:
 * - Stream API: fromElements(...).map(...).filter(...).print().execute()
 * - Operator chain: 每个 operator 将输出直接 push 到下游 (同步调用)
 * - Source 支持同步推送或异步推送（演示用）
 *
 * 目的：帮助理解 Flink 中的 StreamTask、OperatorChain、StreamRecord、算子链等概念
 */
public class MiniFlink {

    /* --------------------------
     * API 接口与基础类型
     * -------------------------- */

    // 包装流记录（保留扩展点，如 timestamp 等）
    public static class StreamRecord<T> {
        public final T value;
        public final long timestamp;
        public StreamRecord(T value) { this(value, System.currentTimeMillis()); }
        public StreamRecord(T value, long ts) { this.value = value; this.timestamp = ts; }
        public String toString(){ return "StreamRecord(" + value + ", ts=" + timestamp + ")"; }
    }

    // Map 函数接口
    public static interface MapFunction<IN, OUT> {
        OUT map(IN value) throws Exception;
    }

    // Filter 函数接口
    public static interface FilterFunction<T> {
        boolean filter(T value) throws Exception;
    }

    // 抽象算子：接收 StreamRecord<Object>，处理后 push 到下游
    public static abstract class Operator {
        protected Consumer<StreamRecord<?>> downstream;

        public Operator() {}

        public void setDownstream(Consumer<StreamRecord<?>> downstream) {
            this.downstream = downstream;
        }

        // 处理来自上游的元素
        public abstract void process(StreamRecord<?> record) throws Exception;
    }

    /* --------------------------
     * 具体算子实现
     * -------------------------- */

    // Map 算子
    public static class MapOperator<IN, OUT> extends Operator {
        private final MapFunction<IN, OUT> func;

        public MapOperator(MapFunction<IN, OUT> f) { this.func = f; }

        @SuppressWarnings("unchecked")
        @Override
        public void process(StreamRecord<?> record) throws Exception {
            IN in = (IN) record.value;
            OUT out = func.map(in);
            if (downstream != null) {
                downstream.accept(new StreamRecord<>(out, record.timestamp));
            }
        }
    }

    // Filter 算子
    public static class FilterOperator<T> extends Operator {
        private final FilterFunction<T> func;

        public FilterOperator(FilterFunction<T> f) { this.func = f; }

        @SuppressWarnings("unchecked")
        @Override
        public void process(StreamRecord<?> record) throws Exception {
            T in = (T) record.value;
            if (func.filter(in)) {
                if (downstream != null) downstream.accept(record);
            }
        }
    }

    // Sink：打印输出
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
        // head 是第一个 operator（可以是 source 直接 push 的入口）
        private Operator head;
        // tail 是当前链的尾 operator（方便拼接下一个算子）
        private Operator tail;

        private Stream() {}

        // 从元素创建一个同步 source
        @SafeVarargs
        public static <T> Stream<T> fromElements(T... elements) {
            Stream<T> s = new Stream<>();
            // source 算子：它不是 Operator 的子类，为了简化直接把 head 设为 null，
            // 实际上我们会保存元素到 sourceElements，并在 execute() 时推送
            s.sourceElements = Arrays.asList(elements);
            return s;
        }

        // 内部存放 source 数据（简化模型）
        private List<T> sourceElements;

        // 添加 map 算子
        public <R> Stream<R> map(MapFunction<T, R> mapFunc) {
            MapOperator<T, R> mapOp = new MapOperator<>(mapFunc);
            attachOperator(mapOp);
            Stream<R> next = new Stream<>();
            next.head = this.head;
            next.tail = mapOp;
            next.sourceElements = (List<R>) this.sourceElements;
            return next;
        }

        // 添加 filter 算子
        public Stream<T> filter(FilterFunction<T> filterFunc) {
            FilterOperator<T> filt = new FilterOperator<>(filterFunc);
            attachOperator(filt);
            this.tail = filt;
            return this;
        }

        // 添加 print sink
        public Stream<T> print() {
            PrintSink sink = new PrintSink();
            attachOperator(sink);
            this.tail = sink;
            return this;
        }

        // 将 operator 连接到链尾
        private void attachOperator(Operator op) {
            if (head == null) {
                // 首次 attach：head 和 tail 都指向 op
                head = op;
                tail = op;
            } else {
                // 将当前 tail 的 downstream 设置为一个 Consumer，负责把 StreamRecord push 到下游 op.process(...)
                Operator prev = tail;
                // 创建 downstream Consumer：调用下一个算子的 process（同步）
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

        // 执行：将 source 的元素逐个推送到算子链 head（同步）
        public void execute() {
            if (head == null) {
                System.out.println("No operators attached. Nothing to execute.");
                return;
            }

            // 将每个 element 包装成 StreamRecord 推给 head（head 的 process 实现会将其下发）
            for (T e : sourceElements) {
                StreamRecord<T> rec = new StreamRecord<>(e);
                try {
                    head.process(rec);
                } catch (Exception ex) {
                    throw new RuntimeException("Error processing record: " + rec, ex);
                }
            }
        }

        // 支持异步 source：把元素放到线程池，模拟流式到来（可选）
        public void executeAsync(long intervalMillis) {
            if (head == null) {
                System.out.println("No operators attached. Nothing to execute.");
                return;
            }
            ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
            final Iterator<T> it = sourceElements.iterator();
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
            // 等待结束（简单处理）
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
                .map((Integer x) -> x * 2)        // map: int -> int
                .filter((Integer x) -> x % 4 == 0) // filter: 保留能被4整除的
                .print()                           // sink: 打印
                .execute();

        // Example 2: 异步/间隔流式执行（每 200ms 发一个）
        System.out.println("\n== Async (interval) execution ==");
        Stream.fromElements("a","bb","ccc","dddd")
                .map((String s) -> s + "[" + s.length() + "]")
                .print()
                .executeAsync(200);
    }
}
