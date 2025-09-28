import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * TinyFlink7 - 极简 Flink 模拟（增加广播/Side Output 支持）
 *
 * 特性：
 * 1. StreamRecord、Operator、OperatorChain 模拟 Flink 核心概念
 * 2. map / filter / print 支持链式调用
 * 3. describeChain() 打印链结构
 * 4. 支持广播/多路输出（一个算子可以把数据发送到多个下游）
 * 5. 全局日志 EXEC_LOG + DEBUG 输出，便于学习调试
 */
public class TinyFlink7 {

    public static boolean DEBUG = true;
    public static List<String> EXEC_LOG = new ArrayList<>();

    public static void enableDebug(boolean enable) { DEBUG = enable; }
    public static void clearLog() { EXEC_LOG.clear(); }
    public static List<String> getExecLog() { return new ArrayList<>(EXEC_LOG); }

    // --------------------------
    // StreamRecord
    // --------------------------
    public static class StreamRecord<T> {
        public final T value;
        public final long timestamp;
        public StreamRecord(T value) { this(value, System.currentTimeMillis()); }
        public StreamRecord(T value, long ts) { this.value = value; this.timestamp = ts; }
        @Override
        public String toString() { return "StreamRecord(" + value + ", ts=" + timestamp + ")"; }
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
        protected List<Consumer<StreamRecord<?>>> downstreams = new ArrayList<>(); // 支持多下游广播
        protected String name;
        protected Operator next; // 用于打印链结构

        public void setName(String name) { this.name = name; }
        public String getName() { return name; }

        public void addDownstream(Consumer<StreamRecord<?>> consumer) { downstreams.add(consumer); }

        // 广播：把 record 发送给所有下游
        protected void pushToDownstreams(StreamRecord<?> record) {
            for (Consumer<StreamRecord<?>> c : downstreams) { c.accept(record); }
        }

        public abstract void process(StreamRecord<?> record) throws Exception;
    }

    // --------------------------
    // MapOperator
    // --------------------------
    public static class MapOperator<IN, OUT> extends Operator {
        private final MapFunction<IN, OUT> func;
        public MapOperator(MapFunction<IN, OUT> f) { this.func = f; this.name = "MapOperator"; }

        @SuppressWarnings("unchecked")
        @Override
        public void process(StreamRecord<?> record) throws Exception {
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
        public FilterOperator(FilterFunction<T> f) { this.func = f; this.name = "FilterOperator"; }

        @SuppressWarnings("unchecked")
        @Override
        public void process(StreamRecord<?> record) throws Exception {
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
        public void process(StreamRecord<?> record) {
            if (DEBUG) System.out.println("[" + name + "] " + record);
            EXEC_LOG.add(name + " " + record);
            if (!DEBUG) System.out.println(record);
        }
    }

    // --------------------------
    // Stream API
    // --------------------------
    public static class Stream<T> {
        private Operator head;
        private Operator tail;
        private List<T> sourceElements;

        public Stream(List<T> elements) { this.sourceElements = elements; }
        public Stream() {}

        // map
        public <R> Stream<R> map(MapFunction<T, R> mapFunc) {
            MapOperator<T, R> mapOp = new MapOperator<>(mapFunc);
            attachOperator(mapOp);
            Stream<R> next = new Stream<>();
            next.head = this.head;
            next.tail = mapOp;
            next.sourceElements = Collections.emptyList();
            return next;
        }

        // filter
        public Stream<T> filter(FilterFunction<T> filterFunc) {
            FilterOperator<T> filt = new FilterOperator<>(filterFunc);
            attachOperator(filt);
            this.tail = filt;
            return this;
        }

        // print sink
        public Stream<T> print() {
            PrintSink sink = new PrintSink();
            attachOperator(sink);
            this.tail = sink;
            return this;
        }

        // attachOperator 非函数式
        private void attachOperator(final Operator op) {
            if (head == null) {
                head = op;
                tail = op;
                op.next = null;
            } else {
                final Operator prev = tail;
                prev.addDownstream(new Consumer<StreamRecord<?>>() {
                    @Override
                    public void accept(StreamRecord<?> rec) {
                        try { op.process(rec); } catch (Exception e) { throw new RuntimeException(e); }
                    }
                });
                prev.next = op;
                tail = op;
                op.next = null;
            }
        }

        // --------------------------
        // Side Output（多路输出）示例
        // --------------------------
        private Map<String, List<Operator>> sideOutputs = new HashMap<>();
        public void registerSideOutput(String tag, Operator op) {
            if (!sideOutputs.containsKey(tag)) sideOutputs.put(tag, new ArrayList<Operator>());
            sideOutputs.get(tag).add(op);
        }

        public void pushToSide(String tag, StreamRecord<?> record) {
            List<Operator> list = sideOutputs.get(tag);
            if (list != null) {
                for (Operator o : list) {
                    try { o.process(record); } catch (Exception e) { throw new RuntimeException(e); }
                }
            }
        }

        // describeChain
        public void describeChain(String title) {
            StringBuilder sb = new StringBuilder();
            sb.append("== ").append(title).append(" ==\n");
            if (head == null) sb.append("  [empty chain]\n");
            else {
                Operator cur = head; int idx = 0;
                while (cur != null) {
                    sb.append("   [").append(idx).append("] ").append(cur.getName()).append("\n");
                    cur = cur.next; idx++;
                }
                sb.append("   (end of chain)\n");
            }
            System.out.print(sb.toString());
        }

        public void describeChain() { describeChain("Current Operator Chain"); }

        // execute sync
        public void execute() {
            if (head == null) { System.out.println("No operators attached."); return; }
            for (T e : sourceElements) {
                StreamRecord<T> rec = new StreamRecord<>(e);
                try { head.process(rec); }
                catch (Exception ex) { throw new RuntimeException("Error processing record: " + rec, ex); }
            }
        }

        // execute async
        public void executeAsync(long intervalMillis) {
            if (head == null) { System.out.println("No operators attached."); return; }
            ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
            final Iterator<T> it = sourceElements.iterator();
            exec.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    if (!it.hasNext()) { exec.shutdown(); return; }
                    T e = it.next();
                    StreamRecord<T> rec = new StreamRecord<>(e);
                    try { head.process(rec); } catch (Exception ex) { ex.printStackTrace(); }
                }
            }, 0, intervalMillis, TimeUnit.MILLISECONDS);
            try { exec.awaitTermination(10, TimeUnit.SECONDS); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
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
    // main 示例
    // --------------------------
    public static void main(String[] args) {
        TinyFlink7.enableDebug(true);
        TinyFlink7.clearLog();

        StreamExecutionEnvironment env = new StreamExecutionEnvironment();

        // Step 1: Source
        Stream<Integer> source = env.fromElements(1, 2, 3, 4, 5);
        source.describeChain("Step 1: Source created");

        // Step 2: Map
        Stream<Integer> mapped = source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer x) { return x * 2; }
        });
        mapped.describeChain("Step 2: After map");

        // Step 3: Filter
        Stream<Integer> filtered = mapped.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer x) { return x % 4 == 0; }
        });
        filtered.describeChain("Step 3: After filter");

        // Step 4: Print Sink
        Stream<Integer> result = filtered.print();
        result.describeChain("Step 4: After print (sink attached)");

        // Step 5: 执行同步流
        System.out.println("== Execute ==");
        result.execute();

        // Step 6: Side Output 示例
        // 创建一个 MapOperator 用于 side output
        MapOperator<Integer, String> sideMap = new MapOperator<>(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer x) { return "Side[" + x + "]"; }
        });
        // 注册 side output
        filtered.registerSideOutput("sideTag", sideMap);
        // 将记录推送到 side output
        for (Integer x : Arrays.asList(1,2,3,4,5)) {
            filtered.pushToSide("sideTag", new StreamRecord<>(x));
        }

        System.out.println("\n== Execution Log ==");
        for (String log : TinyFlink7.getExecLog()) {
            System.out.println(log);
        }

        // Step 7: 打印 side output
        System.out.println("\n== Side Output Processed ==");
        // 注意 sideMap 已经处理完，结果在 EXEC_LOG 或控制台
    }
}
