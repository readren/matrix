package readren.common.collections;

public abstract class Node<E> {
    volatile Node<E> next;

    /**
     * Constructs a dead dummy node.
     */
    public Node() {
    }

    void appendRelaxed(Node<E> next) {
        // assert next != null;
        // assert this.next == null;
        ConcurrentQueue.NEXT.set(this, next);
    }
}
