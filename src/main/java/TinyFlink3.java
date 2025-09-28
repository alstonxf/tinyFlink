import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * TinyFlink2 - 极简流处理引擎示例（更详细的 attach 注释与链路可视化）
 *
 * 主要改动：
 *  1) 增加 StreamExecutionEnvironment（env.fromElements(...)）
 *  2) 在 Operator 中增加 public Operator next，用于打印链结构（教学用途）
 *  3) 在 attachOperator 中添加逐步说明、在 DEBUG 模式下打印“链变更前/后”的状态
 *
 * 说明：next 字段仅用于可视化/教学；真正的数据传递仍通过 downstream.accept(...) -> next.process(...) 完成
 */
public class TinyFlink3 {

    // --------------------------
    // 全局执行日志 & 调试
    // --------------------------
    public static boolean DEBUG = true;                // 控制调试打印（以及链可视化）
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
    // 用户函数接口：Map / Filter
    // --------------------------
    public interface MapFunction<IN, OUT> { OUT map(IN value) throws Exception; }
    public interface FilterFunction<T> { boolean filter(T value) throws Exception; }

    // --------------------------
    // 抽象算子（Operator）
    // --------------------------
    public static abstract class Operator {
        // 当一个算子处理完会调用 downstream.accept(rec) 把数据传给下游（内部实现是调用下一个算子的 process）
        protected Consumer<StreamRecord<?>> downstream;

        // 为教学目的：把链结构显式化，便于打印链的每个节点；实际数据传递仍用 downstream。
        // 这个字段仅用于调试/打印，能让我们顺着 next 遍历链结构。
        public Operator next;

        // 算子名称（便于打印）
        protected String name;

        public void setDownstream(Consumer<StreamRecord<?>> downstream) { this.downstream = downstream; }
        public void setName(String name) { this.name = name; }
        public String getName() { return name; }

        // 每个算子必须实现 process
        public abstract void process(StreamRecord<?> record) throws Exception;
    }

    // --------------------------
    // MapOperator
    // --------------------------
    public static class MapOperator<IN, OUT> extends Operator {
        private final MapFunction<IN, OUT> func;

        public MapOperator(MapFunction<IN, OUT> f) {
            this.func = f;
            this.name = "Operator";
        }

        @SuppressWarnings("unchecked")
        @Override
        public void process(StreamRecord<?> record) throws Exception {
            IN in = (IN) record.value;
            if (DEBUG) System.out.println("[" + name + "] input=" + in);
            EXEC_LOG.add(name + " input=" + in);

            // 用户函数
            OUT out = func.map(in);

            if (DEBUG) System.out.println("[" + name + "] output=" + out);
            EXEC_LOG.add(name + " output=" + out);

            // 把新 record 发给下游（如果下游存在）
            // 注意：downstream.accept(...) 实际会触发下一个算子的 process(...)（这是我们在 attach 时设置的）
            if (downstream != null) {
                downstream.accept(new StreamRecord<>(out, record.timestamp));
            }
        }
    }

    // --------------------------
    // FilterOperator
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

            // 仅在 keep == true 时才把 record 传给下游
            if (keep && downstream != null) {
                downstream.accept(record);
            }
        }
    }

    // --------------------------
    // PrintSink（终点）
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
    // Stream API（包含 attachOperator 的详细注释）
    // --------------------------
    public static class Stream<T> {
        // head/tail：维护链结构
        private Operator head;
        private Operator tail;

        // 源数据
        private List<T> sourceElements;

        // 私有构造：通过 env.fromElements(...) 创建
        private Stream(List<T> elements) {
            this.sourceElements = elements;
        }

        public <R> Stream<R> map(MapFunction<T, R> mapFunc) {
            MapOperator<T, R> mapOp = new MapOperator<>(mapFunc);
            attachOperator(mapOp);
            Stream<R> next = new Stream<>((List<R>) this.sourceElements);
            next.head = this.head;
            next.tail = mapOp;
            return next;
        }

        public Stream<T> filter(FilterFunction<T> filterFunc) {
            FilterOperator<T> filt = new FilterOperator<>(filterFunc);
            attachOperator(filt);
            this.tail = filt;
            return this;
        }

        public Stream<T> print() {
            PrintSink sink = new PrintSink();
            attachOperator(sink);
            this.tail = sink;
            return this;
        }

        /**
         * attachOperator — 把一个新的算子追加到链尾（最重要的部分）
         *
         * 目标：把 op 加到现有链的尾部，同时设置上一个算子的 downstream，
         *       使得上一个算子在处理完成后会调用下一个算子的 process(...)。
         *
         * 我们在这里做了两件事：
         *  1) 使用 prev.setDownstream(...)：把 prev 的 downstream 设置为一个 Consumer，
         *     该 Consumer 在被调用时会直接调用 op.process(rec)（并捕获异常）
         *  2) 为教学方便，我们同时把 prev.next = op，用于链路可视化与打印。
         *
         * 关键点（call flow）：
         *   head.process(rec) -> head 内部处理 -> head.downstream.accept(newRec)
         *                                 -> nextOperator.process(newRec)
         *
         * 附：我们保持数据传递逻辑（downstream）不变；next 字段仅用于可视化（不会改变行为）
         */
        private void attachOperator(Operator op) {
            // --- DEBUG：在 attach 前打印当前链的状态（便于对比） ---
            debugPrintChain(">> attachOperator: about to attach [" + op.getName() + "] -- current chain:");

            if (head == null) {
                // 情况 A：当前链为空（还没有任何算子）
                // 1) 把新算子设为 head 和 tail（链中只有这个节点）
                //    Before: empty
                //    After : head/tail -> [op]
                head = op;
                tail = op;

                // next 字段保持 null（没有下一个）
                op.next = null;

                if (DEBUG) {
                    System.out.println("  (chain was empty) -> set head and tail to [" + op.getName() + "]");
                    debugPrintChain("  after attaching (was empty):");
                }
            } else {
                // 情况 B：链中已有算子，我们把新算子追加到链尾
                // prev 指向旧的尾节点（old tail）
                Operator prev = tail;

                // Step 1: 在旧尾节点 prev 上设置 downstream。
                // 当 prev 处理完一条记录时，会调用 downstream.accept(rec)
                // 我们这里把 downstream 设置为：rec -> op.process(rec)
                // 也就是说，downstream.accept -> op.process(...)，完成串行的调用链。
                prev.setDownstream(new Consumer<StreamRecord<?>>() {
                    @Override
                    public void accept(StreamRecord<?> rec) {
                        try {
                            // 直接调用下一个算子的 process（同步调用）
                            op.process(rec);
                        } catch (Exception e) {
                            // 把 checked exception 包装成 RuntimeException 向上抛（教学示例中简化错误处理）
                            throw new RuntimeException(e);
                        }
                    }
                });

                // Step 2（教学专用）：把 prev.next 指向 op，使得我们可以通过 next 链遍历链结构并打印。
                // 这对于理解“链的形状”和调试非常有帮助。
                prev.next = op;

                // Step 3：更新 tail 为新的末端 op
                tail = op;

                // Step 4：新加入节点的 next 默认为 null（因为它是新的尾）
                op.next = null;

                // DEBUG: 打印本次 attach 的详细步骤和链的新状态
                if (DEBUG) {
                    System.out.println("  (attached new operator) prev=[" + prev.getName() + "] -> now links to -> [" + op.getName() + "]");
                    debugPrintChain("  after attaching:");
                }
            }
        }

        // 辅助：在 DEBUG 模式下打印链中每个节点（从 head 开始）
        // 我们根据 next 字段遍历链并打印每一项（index + name）
        private void debugPrintChain(String title) {
            if (!DEBUG) return;
            System.out.println(title);
            if (head == null) {
                System.out.println("  [empty chain]");
                return;
            }
            Operator cur = head;
            int idx = 0;
            while (cur != null) {
                System.out.println("   [" + idx + "] " + cur.getName());
                cur = cur.next;
                idx++;
            }
            System.out.println("   (end of chain)");
        }

        // 执行逻辑（同步）
        public void execute() {
            if (head == null) {
                System.out.println("No operators attached. Nothing to execute.");
                return;
            }
            for (T e : sourceElements) {
                StreamRecord<T> rec = new StreamRecord<>(e);
                try {
                    // 触发链头的处理 —— 随后通过 downstream 串联触发后续算子
                    head.process(rec);
                } catch (Exception ex) {
                    throw new RuntimeException("Error processing record: " + rec, ex);
                }
            }
        }

        // 异步执行（教学用）
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

            try {
                exec.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // --------------------------
    // StreamExecutionEnvironment（用于创建 Stream）
    // --------------------------
    public static class StreamExecutionEnvironment {
        public <T> Stream<T> fromElements(T... elements) {
            return new Stream<>(Arrays.asList(elements));
        }
    }

    // --------------------------
    // Main：示例
    // --------------------------
    public static void main(String[] args) {
        TinyFlink2.enableDebug(true);
        TinyFlink2.clearLog();

        StreamExecutionEnvironment env = new StreamExecutionEnvironment();

        // Step 1: 从元素创建数据流
        Stream<Integer> ds = env.fromElements(1, 2, 3, 4, 5);

        System.out.println("== Sync execution ==");

        // Step 2: 添加 map 算子
        Stream<Integer> mapped = ds.map(x -> x * 2);

        // Step 3: 添加 filter 算子
        Stream<Integer> filtered = mapped.filter(x -> x % 4 == 0);

        // Step 4: 添加 sink（print）
        Stream<Integer> result = filtered.print();

        // Step 5: 执行
        result.execute();
    }

}
