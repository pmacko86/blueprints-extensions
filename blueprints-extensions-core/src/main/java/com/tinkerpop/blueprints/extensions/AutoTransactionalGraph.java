package com.tinkerpop.blueprints.extensions;

import com.tinkerpop.blueprints.TransactionalGraph;


/**
 * A graph with automatic transaction control
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public interface AutoTransactionalGraph extends TransactionalGraph {

	
	/**
	 * Enable or disable automatic transaction control
	 * 
	 * @param enable whether to enable or disable the automatic transaction control
	 */
	public void setAutoTransactionControl(boolean enable);

	
	/**
	 * Get the current size of the transaction buffer
	 * 
	 * @return the current size of the transaction buffer
	 */
	public int getCurrentBufferSize();

	
	/**
	 * Get the maximum size of the transaction buffer
	 * 
	 * @return the maximum size of the transaction buffer
	 */
	public int getMaxBufferSize();

	
	/**
	 * Set the maximum size of the transaction buffer
	 * 
	 * @param size the maximum size of the transaction buffer
	 */
	public void setMaxBufferSize(int size);
}
