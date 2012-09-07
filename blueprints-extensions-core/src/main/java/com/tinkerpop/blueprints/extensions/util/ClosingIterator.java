package com.tinkerpop.blueprints.extensions.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.tinkerpop.blueprints.CloseableIterable;


/**
 * An iterator that closes itself when it's done.
 * 
 * @author Peter Macko (pmacko@eecs.harvard.edu)
 *
 * @param <T> the collection type
 */
public class ClosingIterator<T> implements Iterator<T>, CloseableIterable<T> {
	
	private Iterable<T> iterable;
	private Iterator<T> iterator;
	private boolean lastHasNext;
	private boolean open;
	private boolean iteratorReturned;
	
	
	/**
	 * Create an instance of class ClosingIterator.
	 * 
	 * @param iterable the Iterable object
	 */
	public ClosingIterator(Iterable<T> iterable) {
		this.iterable = iterable;
		this.iterator = this.iterable.iterator();
		this.lastHasNext = true;
		this.open = true;
		this.iteratorReturned = false;
	}
	
	
	/**
	 * Finalize.
	 */
	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}

	
	/**
	 * Return true if it has a next element. Automatically close the wrapped
	 * Iterable if there are no more elements, and it is CloseableIterable.
	 * 
	 * @return true if it has a next element
	 */
	@Override
	public boolean hasNext() {
		if (!open) return false;
		
		lastHasNext = iterator.hasNext();
		if (!lastHasNext) {
			if (iterable instanceof CloseableIterable<?>) {
				((CloseableIterable<?>) iterable).close();
				open = false;
			}
		}
		
		return lastHasNext;
	}
	

	/**
	 * Get the next element.
	 * 
	 * @return the next element
	 */
	@Override
	public T next() {
		if (!open) throw new NoSuchElementException();
		return iterator.next();
	}
	

	/**
	 * Remove the element that was fetched last.
	 * 
	 */
	@Override
	public void remove() {
		iterator.remove();
	}
	

	/**
	 * Get the iterator for this Iterable.
	 * 
	 * @return the iterator
	 */
	@Override
	public Iterator<T> iterator() {
		if (!open) throw new IllegalStateException("The Iterable is already closed");
		if (!iteratorReturned) {
			iteratorReturned = true;
			return this;
		}
		else {
			return new ClosingIterator<T>(iterable);
		}
	}
	

	/**
	 * Close the wrapped Iterable if it is CloseableIterable.
	 */
	@Override
	public void close() {
		open = false;
		if (iterable instanceof CloseableIterable<?>) {
			((CloseableIterable<?>) iterable).close();
		}
	}
}
