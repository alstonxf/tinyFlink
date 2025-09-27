import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * TinyFlink2 - 极简流处理引擎示例（带全局执行日志和详细注释）
 *
 * 目的：
 * 1. 演示 Flink 核心概念：StreamRecord、Operator、OperatorChain、Source->Transformation->Sink
 * 2. 支持同步和异步执行
 * 3. 带全局执行日志，可用于单元测试断言
 */
public class TinyFlink2 {

    // --------------------------
    // 调试开关 & 全局执行日志
    // --------------------------
    public static boolean DEBUG = true; // 控制控制台打印
    public static List<String> EXEC_LOG = new ArrayList<>(); // 全局执行日志

    // 开启/关闭调试打印
    public static void enableDebug(boolean enable) {
        DEBUG = enable;
    }

    // 清空日志
    public static void clearLog() {
        EXEC_LOG.clear();
    }

    // 获取日志（返回副本，避免外部修改）
    public static List<String> getExecLog() {
        return new ArrayList<>(EXEC_LOG);
    }

    // --------------------------
    // 核心数据结构
    // --------------------------

    /**
     * StreamRecord：封装流中的一条数据
     * 包含 value（数据值）和 timestamp（时间戳）
     * 可以扩展更多属性，如 watermark
     */
    public static class StreamRecord<T> {
        public final T value;
        public final long timestamp;

        public StreamRecord(T value) {
            this(value, System.currentTimeMillis()); // 默认使用当前时间戳
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
    // 函数接口
    // --------------------------

    /**
     * MapFunction：接收一个输入值，返回一个输出值
     */
    public static interface MapFunction<IN, OUT> {
        OUT map(IN value) throws Exception;
    }

    /**
     * FilterFunction：接收一个输入值，返回是否保留
     */
    public static interface FilterFunction<T> {
        boolean filter(T value) throws Exception;
    }

    // --------------------------
    // 抽象算子
    // --------------------------

    /**
     * Operator：所有算子的基类
     * 包含下游 Consumer，用于将处理结果推送给下游算子
     */
    public static abstract class Operator {
        protected Consumer<StreamRecord<?>> downstream; // 下游算子
        protected String name; // 算子名称，用于日志记录

        public void setDownstream(Consumer<StreamRecord<?>> downstream) {
            this.downstream = downstream;
        }

        public void setName(String name) {
            this.name = name;
        }

        public abstract void process(StreamRecord<?> record) throws Exception;
    }

    // --------------------------
    // 具体算子实现
    // --------------------------

    /**
     * MapOperator：对输入数据做转换
     */
    public static class MapOperator<IN, OUT> extends Operator {
        private final MapFunction<IN, OUT> func;

        public MapOperator(MapFunction<IN, OUT> f) {
            this.func = f;
            this.name = "MapOperator";
        }

        @SuppressWarnings("unchecked")
        @Override
        public void process(StreamRecord<?> record) throws Exception {
            // 强转输入类型
            IN in = (IN) record.value;

            // 打印调试信息 & 记录全局日志
            if (DEBUG) System.out.println("[" + name + "] input=" + in);
            EXEC_LOG.add(name + " input=" + in);

            // 执行 map 函数
            OUT out = func.map(in);

            if (DEBUG) System.out.println("[" + name + "] output=" + out);
            EXEC_LOG.add(name + " output=" + out);

            // 推送到下游算子
            if (downstream != null) {
                downstream.accept(new StreamRecord<>(out, record.timestamp));
            }
        }
    }

    /**
     * FilterOperator：根据条件保留或丢弃数据
     */
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

            // 打印调试信息 & 记录日志
            if (DEBUG) System.out.println("[" + name + "] value=" + in + " -> " + keep);
            EXEC_LOG.add(name + " value=" + in + " -> " + keep);

            // 保留才推送到下游
            if (keep && downstream != null) {
                downstream.accept(record);
            }
        }
    }

    /**
     * PrintSink：打印数据到控制台（终点算子）
     */
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
    // Stream API：构建算子链
    // --------------------------

    public static class Stream<T> {
        private Operator head; // 链头
        private Operator tail; // 链尾
        private List<T> sourceElements; // Source 数据

        private Stream() {}

        // 创建流 Source
        @SafeVarargs
        public static <T> Stream<T> fromElements(T... elements) {
            Stream<T> s = new Stream<>();
            s.sourceElements = Arrays.asList(elements);
            return s;
        }

        // 添加 Map 算子
        public <R> Stream<R> map(MapFunction<T, R> mapFunc) {
            MapOperator<T, R> mapOp = new MapOperator<>(mapFunc);
            attachOperator(mapOp);

            // 返回新 Stream，类型改变
            Stream<R> next = new Stream<>();
            next.head = this.head;
            next.tail = mapOp;
            next.sourceElements = (List<R>) this.sourceElements;
            return next;
        }

        // 添加 Filter 算子
        public Stream<T> filter(FilterFunction<T> filterFunc) {
            FilterOperator<T> filt = new FilterOperator<>(filterFunc);
            attachOperator(filt);
            this.tail = filt;
            return this;
        }

        // 添加 Sink
        public Stream<T> print() {
            PrintSink sink = new PrintSink();
            attachOperator(sink);
            this.tail = sink;
            return this;
        }

        /**
         * 连接算子链：把新的算子接到当前链的尾部 (tail)
         *
         * 在 TinyFlink 中，一个 Stream 由一系列 Operator 组成（source -> map -> filter -> sink）。
         * attachOperator 的作用是把新的算子 (op) 追加到链的末尾：
         * - 如果这是第一个算子，则 head 和 tail 都指向它；
         * - 否则，就把当前 tail 的下游 (downstream) 设置为 “调用新算子的 process 方法”，
         *   然后更新 tail = 新算子。
         */
        private void attachOperator(Operator op) {
            if (head == null) {
                // 如果链是空的（还没有任何算子）
                // 那么当前 op 就是第一个算子，同时也是最后一个算子
                head = op;
                tail = op;
            } else {
                // 否则，说明链中已经有至少一个算子了
                Operator prev = tail; // 保存原来的链尾

                /**
                 * 将上一个算子的 downstream 设置为 “调用下一个算子 (op) 的 process 方法”。
                 *
                 * downstream 是一个 Consumer<StreamRecord<?>>，
                 * 代表 “当上游算子处理完数据后，把结果交给下游怎么处理”。
                 *
                 * 这里我们用匿名内部类实现 Consumer 接口：
                 * - accept(rec) 方法会调用 op.process(rec)
                 * - 这样，上游算子执行完之后，就会触发下游算子的 process 方法
                 */
                prev.setDownstream(new Consumer<StreamRecord<?>>() {
                    @Override
                    public void accept(StreamRecord<?> rec) {
                        try {
                            // 把数据交给下一个算子处理
                            op.process(rec);
                        } catch (Exception e) {
                            // 如果下游算子处理出错，包装成 RuntimeException 抛出
                            throw new RuntimeException(e);
                        }
                    }
                });

                // 更新链的尾部：现在新的算子变成 tail
                tail = op;
            }
        }


        // --------------------------
        // 执行方法
        // --------------------------

        // 同步执行：一次性推送所有元素
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

        // 异步执行：定时推送元素
//        public void executeAsync(long intervalMillis) {
//            if (head == null) {
//                System.out.println("No operators attached. Nothing to execute.");
//                return;
//            }
//
//            ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
//            final Iterator<T> it = sourceElements.iterator();
//
//            exec.scheduleAtFixedRate(new Runnable() {
//                @Override
//                public void run() {
//                    if (!it.hasNext()) {
//                        exec.shutdown();
//                        return;
//                    }
//                    T e = it.next();
//                    StreamRecord<T> rec = new StreamRecord<>(e);
//                    try {
//                        head.process(rec);
//                    } catch (Exception ex) {
//                        ex.printStackTrace();
//                    }
//                }
//            }, 0, intervalMillis, TimeUnit.MILLISECONDS);
//
//            try {
//                exec.awaitTermination(10, TimeUnit.SECONDS);
//            } catch (InterruptedException ie) {
//                Thread.currentThread().interrupt();
//            }
//        }
    }

    // --------------------------
    // Main 示例
    // --------------------------

    public static void main(String[] args) {
        TinyFlink2.enableDebug(true);
        TinyFlink2.clearLog();

        System.out.println("== Sync execution ==");
        Stream.fromElements(1, 2, 3, 4, 5)
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer x) {
                        return x * 2;
                    }
                })
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer x) {
                        return x % 4 == 0;
                    }
                })
                .print()
                .execute();

//        System.out.println("\n== Async (interval) execution ==");
//        Stream.fromElements("a", "bb", "ccc", "dddd")
//                .map(new MapFunction<String, String>() {
//                    @Override
//                    public String map(String s) {
//                        return s + "[" + s.length() + "]";
//                    }
//                })
//                .print()
//                .executeAsync(200);

        // --------------------------
        // 打印全局执行日志
        // --------------------------
        System.out.println("\n== Execution Log ==");
        for (String log : TinyFlink2.getExecLog()) {
            System.out.println(log);
        }
    }
}
