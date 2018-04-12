package knn.graph;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * This class implements a bounded priority queue
 * A structure that always keeps the n 'largest' elements
 * 
 * @author tibo
 * @param <E>
 */
public class BoundedPriorityQueue<E> extends PriorityQueue<E> {
    
//    public static void main(String [] args) {
//        BoundedPriorityQueue<Integer> q = new BoundedPriorityQueue(4, new Comparator<Integer>
//        {
//        	
//        });
//        q.add(1);
//        q.add(4);
//        q.add(5);
//        q.add(6);
//        q.add(2);
//        
//        System.out.println(q);
//    }
    
    /**
	 * 
	 */
	private static final long serialVersionUID = -8789341157399903122L;
	protected int CAPACITY = Integer.MAX_VALUE;
    private final Comparator<E> _comparator;
    
    /**
     * Create a bounded priority queue with given maximum capacity
     * @param capacity 
     */
    public BoundedPriorityQueue(int capacity, Comparator<E> comparator_) {
        super(capacity, comparator_);
        _comparator = comparator_;
        this.CAPACITY = capacity;
    }

    /**
     * Creates a priority queue with maximum capacity Integer.MAX_VALUE
     */
    public BoundedPriorityQueue(Comparator<E> comparator_) {
        super(10, comparator_);
        _comparator = comparator_;
    }
    
    /**
     * When the queue is full, adds the element if it is larger than the smallest
     * element already in the queue.
     * 
     * It the element is not comparable, throws a ClassCastException
     * 
     * @param element
     * @return true if element was added
     */
    @Override
    public boolean add(E element) {
        if (! (element instanceof java.lang.Comparable) ) {
            throw new ClassCastException();
        }
        
        if (this.contains(element)) {
            return false;
        }
        
        if (this.size() < CAPACITY) {
            return super.add(element);
        }
        
        if (_comparator.compare(element, peek()) > 0)
        {
            this.poll();
            return super.add(element);
        }
        
        return false;
    }
}