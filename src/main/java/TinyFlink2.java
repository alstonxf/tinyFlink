import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * TinyFlink2 - 极简流处理引擎示例（带全局执行日志和超详细注释）
 *
 * 设计目标（简化版 Flink）：
 *  1) 演示流计算核心抽象：StreamRecord、Operator、算子链（operator chain）
 *  2) 演示数据在算子链内同步传递（单线程模型，类似 StreamTask 串行执行）
 *  3) 提供调试输出与全局执行日志（EXEC_LOG）以便单元测试断言与学习观察
 *
 * 注意：
 *  - 本实现为教学/演示用途，简化了很多生产系统必须考虑的部分（并发、backpressure、网络、状态、容错等）
 *  - fromElements 使用了泛型可变参数 (T... elements)，并标注 @SafeVarargs（在真实项目使用时请注意堆污染风险）
 */
public class TinyFlink2 {

    // --------------------------
    // 全局配置与执行日志
    // --------------------------
    // DEBUG 控制台打印：true -> 算子内部打印详细信息；false -> 只在 sink 最终打印基本输出
    public static boolean DEBUG = true;

    // 全局执行日志：记录每条记录在每个算子处的关键事件（便于单元测试断言）
    // 按生产环境应避免使用静态可变集合，这里用于教学示例
    public static List<String> EXEC_LOG = new ArrayList<>();

    // 开/关调试打印（方便在主程序中控制）
    public static void enableDebug(boolean enable) {
        DEBUG = enable;
    }

    // 清空全局日志（每次运行前调用）
    public static void clearLog() {
        EXEC_LOG.clear();
    }

    // 获取当前执行日志的副本（防止外部直接修改 EXEC_LOG）
    public static List<String> getExecLog() {
        return new ArrayList<>(EXEC_LOG);
    }

    // --------------------------
    // 核心数据结构（StreamRecord）
    // --------------------------

    /**
     * StreamRecord：对流中一条数据的包装
     * - value：实际携带的数据（泛型）
     * - timestamp：记录时间戳（这里用当前系统时间模拟）
     *
     * 注意：
     *  - 在真实 Flink 中，StreamRecord 还可能携带 Watermark、headers 等信息
     *  - toString 用于打印和 EXEC_LOG 记录
     */
    public static class StreamRecord<T> {
        public final T value;
        public final long timestamp;

        public StreamRecord(T value) {
            // 调用构造函数链：默认使用当前时间戳
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
    // 用户函数接口（Map / Filter）
    // --------------------------

    /**
     * MapFunction：把输入 IN 映射为输出 OUT
     * 类似 Flink 的 MapFunction
     */
    public static interface MapFunction<IN, OUT> {
        OUT map(IN value) throws Exception;
    }

    /**
     * FilterFunction：根据输入决定是否保留（true 保留，false 丢弃）
     */
    public static interface FilterFunction<T> {
        boolean filter(T value) throws Exception;
    }

    // --------------------------
    // 抽象算子（Operator）
    // --------------------------

    /**
     * Operator：算子基类
     * - 每个算子有一个下游 consumer（downstream），当算子处理完一条 record 后会调用 downstream.accept(record)
     * - name 字段用于日志标识（便于 EXEC_LOG 跟踪）
     *
     * 设计含义：
     *  - downstream 使用 Consumer<StreamRecord<?>> 作为通用适配器，
     *    在 attachOperator 时，我们会把 downstream 设置成一个调用下一个算子 process 的适配器。
     */
    public static abstract class Operator {
        // 下游接收者：上游通过 downstream.accept(...) 把 record 推给下游
        protected Consumer<StreamRecord<?>> downstream;
        // 算子名称（便于日志）
        protected String name;

        public void setDownstream(Consumer<StreamRecord<?>> downstream) {
            this.downstream = downstream;
        }

        public void setName(String name) {
            this.name = name;
        }

        // 每个具体算子必须实现的处理方法
        public abstract void process(StreamRecord<?> record) throws Exception;
    }

    // --------------------------
    // 具体算子实现（Map / Filter / Sink）
    // --------------------------

    /**
     * MapOperator：执行 MapFunction，将 IN -> OUT，并把结果推给下游
     * 关键点：
     *  - 这里存在一次不安全的强转 (IN in = (IN) record.value)
     *    该强转在本示例中是可控的（由 fromElements 和 map 函数的类型参数保证）
     *  - 处理完后通过 downstream.accept(...) 将新 StreamRecord 发送给下游（即触发下游算子的 process）
     */
    public static class MapOperator<IN, OUT> extends Operator {
        private final MapFunction<IN, OUT> func;

        public MapOperator(MapFunction<IN, OUT> f) {
            this.func = f;
            this.name = "Operator";
        }

        @SuppressWarnings("unchecked")
        @Override
        public void process(StreamRecord<?> record) throws Exception {
            // 1) 从 record 中取出输入值（强转为 IN）
            IN in = (IN) record.value;

            // 2) 调试输出 + 写入全局 EXEC_LOG（记录输入）
            if (DEBUG) System.out.println("[" + name + "] input=" + in);
            EXEC_LOG.add(name + " input=" + in);

            // 3) 调用用户提供的 map 函数（可能抛异常）
            OUT out = func.map(in);

            // 4) 调试输出 + 写入全局 EXEC_LOG（记录输出）
            if (DEBUG) System.out.println("[" + name + "] output=" + out);
            EXEC_LOG.add(name + " output=" + out);

            // 5) 推送到下游：构造新的 StreamRecord 并调用 downstream.accept(...)
            // downstream 在 attachOperator 时被设置成：rec -> nextOperator.process(rec)
            if (downstream != null) {
                // 这里 new 一个 StreamRecord，把 out 和原 record 的 timestamp 继续传下去
                downstream.accept(new StreamRecord<>(out, record.timestamp));
            }
        }
    }

    /**
     * FilterOperator：执行 FilterFunction，根据返回值决定是否将 record 传给下游
     * 关键点：
     *  - 保留(record) -> downstream.accept(record)
     *  - 丢弃 -> 什么都不做（下游不会收到这条记录）
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
            // 1) 调用用户提供的过滤函数
            boolean keep = func.filter(in);

            // 2) 打印与记录日志
            if (DEBUG) System.out.println("[" + name + "] value=" + in + " -> " + keep);
            EXEC_LOG.add(name + " value=" + in + " -> " + keep);

            // 3) 只有 keep==true 时才把 record 发送到下游
            if (keep && downstream != null) {
                downstream.accept(record);
            }
        }
    }

    /**
     * PrintSink：终点算子，将 record 打印到控制台并写入 EXEC_LOG
     * 说明：
     *  - 如果 DEBUG true，打印更详细的带算子名日志；
     *  - 如果 DEBUG false，按简单输出（模拟生产环境 sink 的最小行为）
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
    // Stream API：构建算子链（包括 fromElements / map / filter / print / attachOperator）
    // --------------------------

    public static class Stream<T> {
        // 链头 head：第一个算子（当执行时，source 会把 record 交给 head.process）
        private Operator head;
        // 链尾 tail：当前链的最后一个算子（用于 attach 新算子）
        private Operator tail;
        // Source 数据（演示用）：fromElements 收集的元素
        private List<T> sourceElements;

        // 私有构造：只能通过 fromElements 创建 Stream
        private Stream() {}

        /**
         * 创建流 Source（支持可变参数）
         * @SafeVarargs 注解：抑制泛型 + 可变参数的编译器警告（前提是方法实现是安全的）
         *
         * 调用示例：
         *   Stream.fromElements(1,2,3)
         *
         * 内部把可变参数打包为 List 以便后续 iterate
         */
        @SafeVarargs
        public static <T> Stream<T> fromElements(T... elements) {
            Stream<T> s = new Stream<>();
            s.sourceElements = Arrays.asList(elements);
            return s;
        }

        /**
         * 添加 Map 算子并返回新的 Stream（泛型转变）
         *  - 创建 MapOperator
         *  - 调用 attachOperator 将其追加到链尾
         *  - 返回一个类型为 R 的 Stream（head 保持原有链头，tail 指向新加的 mapOp）
         */
        public <R> Stream<R> map(MapFunction<T, R> mapFunc) {
            MapOperator<T, R> mapOp = new MapOperator<>(mapFunc);
            attachOperator(mapOp);

            // 返回新的 Stream（类型发生变化）
            Stream<R> next = new Stream<>();
            next.head = this.head;
            next.tail = mapOp;
            // sourceElements 实际类型相同（这里通过不安全的强转共享）
            next.sourceElements = (List<R>) this.sourceElements;
            return next;
        }

        /**
         * 添加 Filter 算子
         */
        public Stream<T> filter(FilterFunction<T> filterFunc) {
            FilterOperator<T> filt = new FilterOperator<>(filterFunc);
            attachOperator(filt);
            this.tail = filt;
            return this;
        }

        /**
         * 添加打印 Sink（终点）
         */
        public Stream<T> print() {
            PrintSink sink = new PrintSink();
            attachOperator(sink);
            this.tail = sink;
            return this;
        }

        /**
         * 连接算子链：把新的算子追加到链的尾部
         *
         * 执行逻辑（注释已很详细）：
         * 1) 若 head==null：说明链为空（这是第一个算子），则 head=op, tail=op
         * 2) else：链已有算子
         *    - prev = tail：保存旧链尾
         *    - 为 prev 设置 downstream：当 prev 处理完一个 record，调用 downstream.accept(rec)
         *      这里我们把 downstream 设置为一个 Consumer，它会调用 op.process(rec)
         *      （也就是说：downstream.accept(rec) -> op.process(rec)）
         *    - 更新 tail = op
         *
         * 通过上述连接方式，形成一个“函数调用链”：
         *   head.process(rec) -> head 内部处理 -> head.downstream.accept(newRec)
         *                                 -> secondOperator.process(newRec)
         *                                 -> secondOperator.downstream.accept(...)
         */
        private void attachOperator(Operator op) {
            if (head == null) {
                // 初始情况：第一个算子
                head = op;
                tail = op;
            } else {
                // 链中已有算子，把新的 op 接到尾部
                Operator prev = tail; // 当前链尾

                // 设置 prev 的 downstream —— 当 prev 完成时，它会把 record 传给下一个算子 op
                prev.setDownstream(new Consumer<StreamRecord<?>>() {
                    @Override
                    public void accept(StreamRecord<?> rec) {
                        try {
                            // 这里直接调用下游算子的 process 方法（同步调用）
                            op.process(rec);
                        } catch (Exception e) {
                            // 将 checked exception 包装成 RuntimeException 向上抛（简化处理）
                            throw new RuntimeException(e);
                        }
                    }
                });

                // 更新链尾为新加入的算子
                tail = op;
            }
        }

        // --------------------------
        // 执行 API（演示同步执行）
        // --------------------------

        /**
         * 同步执行：一次性把 sourceElements 中的每个元素转换为 StreamRecord 并交给 head 处理
         *
         * 执行要点：
         *  - 单线程、同步：不会并发执行
         *  - head.process(record) 会触发后续整个算子链的同步调用（因为 downstream 被设置成调用下游 process）
         */
        public void execute() {
            if (head == null) {
                System.out.println("No operators attached. Nothing to execute.");
                return;
            }
            // 遍历 source 元素（批量一次性推送）
            for (T e : sourceElements) {
                StreamRecord<T> rec = new StreamRecord<>(e);
                try {
                    // 把 record 交给链头处理，随后链头会把结果通过 downstream 串起来传给下游
                    head.process(rec);
                } catch (Exception ex) {
                    // 简化错误处理：直接抛 RuntimeException，便于观察和调试
                    throw new RuntimeException("Error processing record: " + rec, ex);
                }
            }
        }

        // 异步执行：定时推送元素
        public void executeAsync(long intervalMillis) {
            if (head == null) {
                System.out.println("No operators attached. Nothing to execute.");
                return;
            }

            ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
            final Iterator<T> it = sourceElements.iterator();

            exec.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
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
                }
            }, 0, intervalMillis, TimeUnit.MILLISECONDS);

            try {
                exec.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // --------------------------
    // Main：示例 pipeline 与运行
    // --------------------------

    public static void main(String[] args) {
        // 1) 开启调试打印并清空全局日志（便于每次运行对比）
        TinyFlink2.enableDebug(true);
        TinyFlink2.clearLog();

        System.out.println("== Sync execution ==");
        // 2) 构建 pipeline：source -> map -> filter -> print
        Stream.fromElements(1, 2, 3, 4, 5)
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer x) {
                        return x * 2; // 把每个元素 *2
                    }
                })
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer x) {
                        return x % 4 == 0; // 保留能被4整除的
                    }
                })
                .print()
                .execute();

        System.out.println("\n== Async (interval) execution ==");
        Stream.fromElements("a", "bb", "ccc", "dddd")
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String s) {
                        return s + "[" + s.length() + "]";
                    }
                })
                .print()
                .executeAsync(200);

        // --------------------------
        // 打印全局执行日志
        // --------------------------
        System.out.println("\n== Execution Log ==");
        for (String log : TinyFlink2.getExecLog()) {
            System.out.println(log);
        }
    }
}
