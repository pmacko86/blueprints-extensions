package com.tinkerpop.blueprints.extensions;

import com.tinkerpop.blueprints.Graph;

/**
 * A graph with additional methods that make it easier to bulk-load
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public interface BulkloadableGraph extends Graph {

	/**
	 * Start bulk load mode
	 */
	public void startBulkLoad();
	
	/**
	 * Stop bulk load mode
	 */
	public void stopBulkLoad();

	
	// XXX ???
	
	public int getCurrentBufferSize();

	public int getMaxBufferSize();

	public void setMaxBufferSize(int size);
}
