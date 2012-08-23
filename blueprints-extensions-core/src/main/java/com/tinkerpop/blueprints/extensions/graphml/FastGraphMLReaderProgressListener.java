package com.tinkerpop.blueprints.extensions.graphml;

import com.tinkerpop.blueprints.Graph;


/**
 * The progress listener for the GraphML loader
 * 
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public interface FastGraphMLReaderProgressListener {

	
	/**
	 * Callback for when the given number of vertices and edges were loaded
	 * 
	 * @param graph the graph loaded so far
	 * @param vertices the number of vertices loaded so far
	 * @param edges the number of edges
	 */
	public void inputGraphProgress(final Graph graph, int vertices, int edges);
}
