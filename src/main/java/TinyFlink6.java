import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class TinyFlink6 {

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
    // Operator 抽象类
    // --------------------------
    public static abstract class Operator {
        protected List<Consumer<StreamRecord<?>>> downstreams = new ArrayList<>();
        protected String name;
        protected Operator next;

        public void setName(String name) { this.name = name; }
        public String getName() { return name; }

        public void addDownstream(Consumer<StreamRecord<?>> consumer) {
            downstreams.add(consumer);
        }

        protected void pushToDownstreams(StreamRecord<?> record) {
            for (Consumer<StreamRecord<?>> c : downstreams) {
                c.accept(record);
            }
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
    // Stream
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
                        try {
                            op.process(rec);
                        } catch (Exception e) { throw new RuntimeException(e); }
                    }
                });
                prev.next = op;
                tail = op;
                op.next = null;
            }
        }

        // describeChain
        public void describeChain(String title) {
            StringBuilder sb = new StringBuilder();
            sb.append("== ").append(title).append(" ==\n");
            if (head == null) { sb.append("  [empty chain]\n"); }
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
    // main
    // --------------------------
    public static void main(String[] args) {
        TinyFlink6.enableDebug(true);
        TinyFlink6.clearLog();

        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        Stream<Integer> ds = env.fromElements(1, 2, 3, 4, 5);
        ds.describeChain("Step 1: Source created");

        Stream<Integer> mapped = ds.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer x) { return x * 2; }
        });
        mapped.describeChain("Step 2: after map");

        Stream<Integer> filtered = mapped.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer x) { return x % 4 == 0; }
        });
        filtered.describeChain("Step 3: after filter");

        Stream<Integer> result = filtered.print();
        result.describeChain("Step 4: after print (sink attached)");

        System.out.println("== Execute ==");
        result.execute();

        System.out.println("\n== Execution Log ==");
        for (String log : TinyFlink6.getExecLog()) System.out.println(log);
    }
}
