package readren.matrix.collections;

abstract class Node<E> {
    volatile Node<E> next;

    /**
     * Constructs a dead dummy node.
     */
    Node() {
    }

    void appendRelaxed(Node<E> next) {
        // assert next != null;
        // assert this.next == null;
        ConcurrentQueue.NEXT.set(this, next);
    }
}
