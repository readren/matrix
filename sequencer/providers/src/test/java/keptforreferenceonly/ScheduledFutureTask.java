package readren.sequencer.keptforreferenceonly;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * See {@link HeapQueue}
 */
class ScheduledFutureTask<V>
        extends FutureTask<V> implements RunnableScheduledFuture<V> {

    /** Sequence number to break ties FIFO */
    private final long sequenceNumber;

    /** The nanoTime-based time when the task is enabled to execute. */
    private volatile long time;

    /**
     * Period for repeating tasks, in nanoseconds.
     * A positive value indicates fixed-rate execution.
     * A negative value indicates fixed-delay execution.
     * A value of 0 indicates a non-repeating (one-shot) task.
     */
    private final long period;

    /** The actual task to be re-enqueued by reExecutePeriodic */
    RunnableScheduledFuture<V> outerTask = this;

    /**
     * Index into delay queue, to support faster cancellation.
     */
    int heapIndex;

    /**
     * Creates a one-shot action with given nanoTime-based trigger time.
     */
    ScheduledFutureTask(Runnable r, V result, long triggerTime,
                        long sequenceNumber) {
        super(r, result);
        this.time = triggerTime;
        this.period = 0;
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * Creates a periodic action with given nanoTime-based initial
     * trigger time and period.
     */
    ScheduledFutureTask(Runnable r, V result, long triggerTime,
                        long period, long sequenceNumber) {
        super(r, result);
        this.time = triggerTime;
        this.period = period;
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * Creates a one-shot action with given nanoTime-based trigger time.
     */
    ScheduledFutureTask(Callable<V> callable, long triggerTime,
                        long sequenceNumber) {
        super(callable);
        this.time = triggerTime;
        this.period = 0;
        this.sequenceNumber = sequenceNumber;
    }

    public long getDelay(TimeUnit unit) {
        return unit.convert(time - System.nanoTime(), NANOSECONDS);
    }

    public int compareTo(Delayed other) {
        if (other == this) // compare zero if same object
            return 0;
        if (other instanceof ScheduledFutureTask) {
            ScheduledFutureTask<?> x = (ScheduledFutureTask<?>)other;
            long diff = time - x.time;
            if (diff < 0)
                return -1;
            else if (diff > 0)
                return 1;
            else if (sequenceNumber < x.sequenceNumber)
                return -1;
            else
                return 1;
        }
        long diff = getDelay(NANOSECONDS) - other.getDelay(NANOSECONDS);
        return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
    }

    /**
     * Returns {@code true} if this is a periodic (not a one-shot) action.
     *
     * @return {@code true} if periodic
     */
    public boolean isPeriodic() {
        return period != 0;
    }

    /**
     * Sets the next time to run for a periodic task.
     */
//    private void setNextRunTime() {
//        long p = period;
//        if (p > 0)
//            time += p;
//        else
//            time = triggerTime(-p);
//    }

//    public boolean cancel(boolean mayInterruptIfRunning) {
//        // The racy read of heapIndex below is benign:
//        // if heapIndex < 0, then OOTA guarantees that we have surely
//        // been removed; else we recheck under lock in remove()
//        boolean cancelled = super.cancel(mayInterruptIfRunning);
//        if (cancelled && removeOnCancel && heapIndex >= 0)
//            remove(this);
//        return cancelled;
//    }

    /**
     * Overrides FutureTask version so as to reset/requeue if periodic.
     */
//    public void run() {
//        if (!canRunInCurrentRunState(this))
//            cancel(false);
//        else if (!isPeriodic())
//            super.run();
//        else if (super.runAndReset()) {
//            setNextRunTime();
//            reExecutePeriodic(outerTask);
//        }
//    }
}
