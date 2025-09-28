import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Stream<T> {
    private TinyFlink4.Operator head;
    private TinyFlink4.Operator tail;
    private List<T> sourceElements;

    Stream(List<T> elements) {
        this.sourceElements = elements;
    }

    Stream() {
    }

    public <R> Stream<R> map(TinyFlink4.MapFunction<T, R> mapFunc) {
        TinyFlink4.MapOperator<T, R> mapOp = new TinyFlink4.MapOperator<>(mapFunc);
        attachOperator(mapOp);

        Stream<R> next = new Stream<>();
        next.head = this.head;
        next.tail = mapOp;
        next.sourceElements = Collections.emptyList(); // 不依赖旧 sourceElements
        return next;
    }

    public Stream<T> filter(TinyFlink4.FilterFunction<T> filterFunc) {
        TinyFlink4.FilterOperator<T> filt = new TinyFlink4.FilterOperator<>(filterFunc);
        attachOperator(filt);
        this.tail = filt;
        return this;
    }

    public Stream<T> print() {
        TinyFlink4.PrintSink sink = new TinyFlink4.PrintSink();
        attachOperator(sink);
        this.tail = sink;
        return this;
    }

    /**
     * 非函数式 attachOperator
     * 用匿名内部类替代 lambda
     */
    private void attachOperator(TinyFlink4.Operator op) {
        if (head == null) {
            head = op;
            tail = op;
            op.next = null;
        } else {
            final TinyFlink4.Operator prev = tail;

            // 使用匿名内部类替代 lambda
            prev.setDownstream(new Consumer<TinyFlink4.StreamRecord<?>>() {
                @Override
                public void accept(TinyFlink4.StreamRecord<?> rec) {
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
            TinyFlink4.StreamRecord<T> rec = new TinyFlink4.StreamRecord<>(e);
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

        // 匿名内部类替代 lambda
        Runnable task = new Runnable() {
            @Override
            public void run() {
                if (!it.hasNext()) {
                    exec.shutdown();
                    return;
                }
                T e = it.next();
                TinyFlink4.StreamRecord<T> rec = new TinyFlink4.StreamRecord<>(e);
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
            TinyFlink4.Operator cur = head;
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
