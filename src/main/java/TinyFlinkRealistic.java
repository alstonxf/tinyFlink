import java.util.*;
import java.util.concurrent.*;

/**
 * TinyFlinkRealistic - 极简版 Flink 流处理模拟
 *
 * 特点：
 * 1. 模拟 Flink 核心算子链执行（StreamTask + OperatorChain）
 * 2. 每个算子直接持有下游算子引用，调用 processElement，贴近真实 Flink
 * 3. 支持多下游广播（可模拟 side output / split）
 * 4. 支持 Map、Filter、PrintSink
 * 5. 支持同步执行和异步执行（ScheduledExecutorService）
 * 6. 提供 describeChain 可视化算子链
 * 7. 全局 EXEC_LOG 记录每条记录在链中的流转过程
 */
public class TinyFlinkRealistic {

    // --------------------------
    // 全局配置和执行日志
    // --------------------------
    public static boolean DEBUG = true;
    public static List<String> EXEC_LOG = new ArrayList<>();

    public static void enableDebug(boolean enable) { DEBUG = enable; }
    public static void clearLog() { EXEC_LOG.clear(); }
    public static List<String> getExecLog() { return new ArrayList<>(EXEC_LOG); }

    // --------------------------
    // StreamRecord：每条流记录
    // --------------------------
    public static class StreamRecord<T> {
        public final T value;
        public final long timestamp;

        public StreamRecord(T value) {
            this(value, System.currentTimeMillis());
        }

        public StreamRecord(T value, long ts) {
            this.value = value;
            this.timestamp = ts;
        }

        @Override
        public String toString() {
            return "StreamRecord(" + value + ", ts=" + timestamp + ")";
        }
    }

    // --------------------------
    // 用户函数接口
    // --------------------------
    public interface MapFunction<IN, OUT> { OUT map(IN value) throws Exception; }
    public interface FilterFunction<T> { boolean filter(T value) throws Exception; }

    // --------------------------
    // 抽象算子
    // --------------------------
    public static abstract class Operator {
        protected String name;
        protected List<Operator> downstreams = new ArrayList<>(); // 支持广播 / side output

        public void setName(String name) { this.name = name; }
        public String getName() { return name; }

        // 添加下游算子
        public void addDownstream(Operator op) {
            downstreams.add(op);
        }

        // 核心处理方法，子类实现
        public abstract void processElement(StreamRecord<?> record) throws Exception;

        // 推送到所有下游
        protected void pushToDownstreams(StreamRecord<?> record) throws Exception {
            for (Operator op : downstreams) {
                op.processElement(record);
            }
        }
    }

    // --------------------------
    // MapOperator
    // --------------------------
    public static class MapOperator<IN, OUT> extends Operator {
        private final MapFunction<IN, OUT> func;

        public MapOperator(MapFunction<IN, OUT> func) {
            this.func = func;
            this.name = "MapOperator";
        }

        @SuppressWarnings("unchecked")
        @Override
        public void processElement(StreamRecord<?> record) throws Exception {
            IN in = (IN) record.value;
            if (DEBUG) System.out.println("[" + name + "] input=" + in);
            EXEC_LOG.add(name + " input=" + in);

            OUT out = func.map(in);

            if (DEBUG) System.out.println("[" + name + "] output=" + out);
            EXEC_LOG.add(name + " output=" + out);

            pushToDownstreams(new StreamRecord<>(out, record.timestamp));
        }
    }

    // --------------------------
    // FilterOperator
    // --------------------------
    public static class FilterOperator<T> extends Operator {
        private final FilterFunction<T> func;

        public FilterOperator(FilterFunction<T> func) {
            this.func = func;
            this.name = "FilterOperator";
        }

        @SuppressWarnings("unchecked")
        @Override
        public void processElement(StreamRecord<?> record) throws Exception {
            T in = (T) record.value;
            boolean keep = func.filter(in);
            if (DEBUG) System.out.println("[" + name + "] value=" + in + " -> " + keep);
            EXEC_LOG.add(name + " value=" + in + " -> " + keep);

            if (keep) pushToDownstreams(record);
        }
    }

    // --------------------------
    // PrintSink
    // --------------------------
    public static class PrintSink extends Operator {
        public PrintSink() { this.name = "PrintSink"; }

        @Override
        public void processElement(StreamRecord<?> record) {
            if (DEBUG) System.out.println("[" + name + "] " + record);
            EXEC_LOG.add(name + " " + record);
        }
    }

    // --------------------------
    // Stream（封装算子链）
    // --------------------------
    public static class Stream<T> {
        private List<T> sourceElements;
        private Operator head; // 链头
        private Operator tail; // 链尾

        public Stream(List<T> elements) { this.sourceElements = elements; }

        // Map算子
        public <R> Stream<R> map(MapFunction<T, R> func) {
            MapOperator<T, R> mapOp = new MapOperator<>(func);
            attachOperator(mapOp);
            return new Stream<R>((List<R>) sourceElements).setHeadTail(head, mapOp);
        }

        // Filter算子
        public Stream<T> filter(FilterFunction<T> func) {
            FilterOperator<T> filterOp = new FilterOperator<>(func);
            attachOperator(filterOp);
            this.tail = filterOp;
            return this;
        }

        // Print Sink
        public Stream<T> print() {
            PrintSink sink = new PrintSink();
            attachOperator(sink);
            this.tail = sink;
            return this;
        }

        // 连接新算子到链尾
        private void attachOperator(Operator op) {
            if (head == null) {
                head = tail = op;
            } else {
                tail.addDownstream(op);
                tail = op;
            }
        }

        // 内部方法，用于 map 生成新 Stream 对象
        private <R> Stream<R> setHeadTail(Operator head, Operator tail) {
            this.head = head;
            this.tail = tail;
            return (Stream<R>) this;
        }

        // --------------------------
        // 执行流
        // --------------------------
        public void execute() {
            if (head == null) {
                System.out.println("No operators attached. Nothing to execute.");
                return;
            }
            for (T e : sourceElements) {
                try {
                    head.processElement(new StreamRecord<>(e));
                } catch (Exception ex) {
                    throw new RuntimeException("Error processing record: " + e, ex);
                }
            }
        }

        // 异步执行
        public void executeAsync(long intervalMillis) {
            if (head == null) return;
            ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
            final Iterator<T> it = sourceElements.iterator();
            exec.scheduleAtFixedRate(() -> {
                if (!it.hasNext()) { exec.shutdown(); return; }
                T e = it.next();
                try { head.processElement(new StreamRecord<>(e)); }
                catch (Exception ex) { ex.printStackTrace(); }
            }, 0, intervalMillis, TimeUnit.MILLISECONDS);
        }

        // 打印链结构
        public void describeChain(String title) {
            System.out.println("== " + title + " ==");
            if (head == null) { System.out.println("  [empty chain]"); return; }
            Operator cur = head;
            int idx = 0;
            Queue<Operator> queue = new LinkedList<>();
            Set<Operator> visited = new HashSet<>();
            queue.add(cur);
            while (!queue.isEmpty()) {
                cur = queue.poll();
                if (visited.contains(cur)) continue;
                visited.add(cur);
                System.out.println("   [" + idx + "] " + cur.getName());
                idx++;
                queue.addAll(cur.downstreams);
            }
            System.out.println("   (end of chain)");
        }
    }

    // --------------------------
    // StreamExecutionEnvironment
    // --------------------------
    public static class StreamExecutionEnvironment {
        public <T> Stream<T> fromElements(T... elements) {
            return new Stream<>(Arrays.asList(elements));
        }
    }

    // --------------------------
    // Main 示例
    // --------------------------
    public static void main(String[] args) {
        TinyFlinkRealistic.enableDebug(true);
        TinyFlinkRealistic.clearLog();

        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        Stream<Integer> ds = env.fromElements(1,2,3,4,5);
        ds.describeChain("Step 1: source created");

        Stream<Integer> mapped = ds.map(new MapFunction<Integer, Integer>() {
            public Integer map(Integer x) { return x * 2; }
        });
        mapped.describeChain("Step 2: after map");

        Stream<Integer> filtered = mapped.filter(new FilterFunction<Integer>() {
            public boolean filter(Integer x) { return x % 4 == 0; }
        });
        filtered.describeChain("Step 3: after filter");

        Stream<Integer> result = filtered.print();
        result.describeChain("Step 4: after print (sink attached)");

        System.out.println("== Execute ==");
        result.execute();

        System.out.println("\n== Execution Log ==");
        for (String log : TinyFlinkRealistic.getExecLog()) {
            System.out.println(log);
        }
    }
}
