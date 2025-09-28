import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * TinyFlink4 - 完全非函数式风格的极简流处理引擎
 *
 * 特性：
 *  - 模拟 Flink 核心概念：StreamRecord、Operator、OperatorChain
 *  - 支持 map / filter / print
 *  - 支持同步执行和异步执行
 *  - 支持 describeChain() 方法展示链的构建过程
 *  - 全局执行日志 EXEC_LOG 可记录算子处理过程 + 链结构
 *  - 完全非函数式写法，匿名内部类替代 lambda
 */
public class TinyFlink4 {

    public static boolean DEBUG = true;
    public static List<String> EXEC_LOG = new ArrayList<>();

    public static void enableDebug(boolean enable) {
        DEBUG = enable;
    }

    public static void clearLog() {
        EXEC_LOG.clear();
    }

    public static List<String> getExecLog() {
        return new ArrayList<>(EXEC_LOG);
    }

    // --------------------------
    // StreamRecord：对流中一条数据的包装
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
    public interface MapFunction<IN, OUT> {
        OUT map(IN value) throws Exception;
    }

    public interface FilterFunction<T> {
        boolean filter(T value) throws Exception;
    }

    // --------------------------
    // 抽象算子（Operator）
    // --------------------------
    public static abstract class Operator {
        protected Consumer<StreamRecord<?>> downstream;
        protected String name;
        protected Operator next; // 用于 describeChain 打印链路

        public void setDownstream(Consumer<StreamRecord<?>> downstream) {
            this.downstream = downstream;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public abstract void process(StreamRecord<?> record) throws Exception;
    }

    // --------------------------
    // Map 算子
    // --------------------------
    public static class MapOperator<IN, OUT> extends Operator {
        private final MapFunction<IN, OUT> func;

        public MapOperator(MapFunction<IN, OUT> f) {
            this.func = f;
            this.name = "MapOperator";
        }

        @SuppressWarnings("unchecked")
        @Override
        public void process(StreamRecord<?> record) throws Exception {
            IN in = (IN) record.value;
            if (DEBUG) System.out.println("[" + name + "] input=" + in);
            EXEC_LOG.add(name + " input=" + in);

            OUT out = func.map(in);

            if (DEBUG) System.out.println("[" + name + "] output=" + out);
            EXEC_LOG.add(name + " output=" + out);

            if (downstream != null) {
                downstream.accept(new StreamRecord<>(out, record.timestamp));
            }
        }
    }

    // --------------------------
    // Filter 算子
    // --------------------------
    public static class FilterOperator<T> extends Operator {
        private final FilterFunction<T> func;

        public FilterOperator(FilterFunction<T> f) {
            this.func = f;
            this.name = "FilterOperator";
        }

        @SuppressWarnings("unchecked")
        @Override
        public void process(StreamRecord<?> record) throws Exception {
            T in = (T) record.value;
            boolean keep = func.filter(in);
            if (DEBUG) System.out.println("[" + name + "] value=" + in + " -> " + keep);
            EXEC_LOG.add(name + " value=" + in + " -> " + keep);

            if (keep && downstream != null) {
                downstream.accept(record);
            }
        }
    }

    // --------------------------
    // Sink 算子
    // --------------------------
    public static class PrintSink extends Operator {
        public PrintSink() {
            this.name = "PrintSink";
        }

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

        public Stream(List<T> elements) {
            this.sourceElements = elements;
        }

        public Stream() {}

        /** Map */
        public <R> Stream<R> map(final MapFunction<T, R> mapFunc) {
            final MapOperator<T, R> mapOp = new MapOperator<T, R>(mapFunc);
            attachOperator(mapOp);

            Stream<R> next = new Stream<R>();
            next.head = this.head;
            next.tail = mapOp;
            next.sourceElements = Collections.emptyList();
            return next;
        }

        /** Filter */
        public Stream<T> filter(final FilterFunction<T> filterFunc) {
            final FilterOperator<T> filt = new FilterOperator<T>(filterFunc);
            attachOperator(filt);
            this.tail = filt;
            return this;
        }

        /** Sink */
        public Stream<T> print() {
            final PrintSink sink = new PrintSink();
            attachOperator(sink);
            this.tail = sink;
            return this;
        }

        /** attachOperator 非函数式写法 */
        private void attachOperator(final Operator op) {
            if (head == null) {
                head = op;
                tail = op;
                op.next = null;
            } else {
                final Operator prev = tail;
                prev.setDownstream(new Consumer<StreamRecord<?>>() {
                    @Override
                    public void accept(StreamRecord<?> rec) {
                        try {
                            op.process(rec);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                prev.next = op;
                tail = op;
                op.next = null;
            }
        }

        /** 同步执行 */
        public void execute() {
            if (head == null) {
                System.out.println("No operators attached. Nothing to execute.");
                return;
            }
            for (T e : sourceElements) {
                StreamRecord<T> rec = new StreamRecord<T>(e);
                try {
                    head.process(rec);
                } catch (Exception ex) {
                    throw new RuntimeException("Error processing record: " + rec, ex);
                }
            }
        }

        /** 非函数式异步执行 */
        public void executeAsync(long intervalMillis) {
            if (head == null) {
                System.out.println("No operators attached. Nothing to execute.");
                return;
            }
            final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
            final Iterator<T> it = sourceElements.iterator();

            Runnable task = new Runnable() {
                @Override
                public void run() {
                    if (!it.hasNext()) {
                        exec.shutdown();
                        return;
                    }
                    T e = it.next();
                    StreamRecord<T> rec = new StreamRecord<T>(e);
                    try {
                        head.process(rec);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            };

            exec.scheduleAtFixedRate(task, 0, intervalMillis, TimeUnit.MILLISECONDS);

            try {
                exec.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        /** 打印链结构 */
        public void describeChain(String title) {
            StringBuilder sb = new StringBuilder();
            sb.append("== ").append(title).append(" ==\n");
            if (head == null) {
                sb.append("  [empty chain]\n");
            } else {
                Operator cur = head;
                int idx = 0;
                while (cur != null) {
                    sb.append("   [").append(idx).append("] ").append(cur.getName()).append("\n");
                    cur = cur.next;
                    idx++;
                }
                sb.append("   (end of chain)\n");
            }
            System.out.print(sb.toString());
        }

        public void describeChain() {
            describeChain("Current Operator Chain");
        }
    }

    // --------------------------
    // 执行环境
    // --------------------------
    public static class StreamExecutionEnvironment {
        public <T> Stream<T> fromElements(T... elements) {
            return new Stream<T>(Arrays.asList(elements));
        }
    }

    // --------------------------
    // Main
    // --------------------------
    public static void main(String[] args) {
        TinyFlink4.enableDebug(true);
        TinyFlink4.clearLog();

        StreamExecutionEnvironment env = new StreamExecutionEnvironment();

        // Source
        Stream<Integer> ds = env.fromElements(1, 2, 3, 4, 5);
        ds.describeChain("Step 1: source created");

        // Map
        Stream<Integer> mapped = ds.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        });
        mapped.describeChain("Step 2: after map");

        // Filter
        Stream<Integer> filtered = mapped.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value % 4 == 0;
            }
        });
        filtered.describeChain("Step 3: after filter");

        // Sink
        Stream<Integer> result = filtered.print();
        result.describeChain("Step 4: after print (sink attached)");

        System.out.println("== Execute ==");
        result.execute();

        System.out.println("\n== Execution Log ==");
        for (String log : TinyFlink4.getExecLog()) {
            System.out.println(log);
        }
    }
}
