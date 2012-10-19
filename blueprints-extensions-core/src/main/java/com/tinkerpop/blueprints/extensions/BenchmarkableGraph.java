package com.tinkerpop.blueprints.extensions;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

/**
 * A graph with additional methods that make it easier to benchmark
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public interface BenchmarkableGraph extends Graph {
    
    /**
     * Count the number of vertices.
     * 
     * @return the number of vertices
     */
    public long countVertices();
    
    /**
     * Return a random vertex.
     * 
     * @return a random vertex
     */
    public Vertex getRandomVertex();

    /**
     * Count the number of edges.
     * 
     * @return the number of edges
     */
    public long countEdges();
    
    /**
     * Return a random edge.
     * 
     * @return a random edge
     */
    public Edge getRandomEdge();
    
    /**
     * Return the buffer pool size.
     * 
     * @return the buffer pool size in MB
     */
    public int getBufferPoolSize();
    
    /**
     * Return the total cache size, including the buffer pool and the session caches.
     * 
     * @return the cache size in MB
     */
    public int getTotalCacheSize();
}
