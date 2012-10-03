package com.tinkerpop.blueprints.extensions.io;


/**
 * The progress listener for a graph reader or writer, or for a streaming graph algorithm
 * 
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public interface GraphProgressListener {

	
	/**
	 * Callback for when the given number of vertices and edges were loaded
	 * 
	 * @param vertices the number of vertices loaded so far
	 * @param edges the number of edges
	 */
	public void graphProgress(int vertices, int edges);
}
