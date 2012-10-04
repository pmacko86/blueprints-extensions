package com.tinkerpop.blueprints.extensions.impls.neo4j;

import java.util.Map;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.BenchmarkableGraph;
import com.tinkerpop.blueprints.impls.neo4jbatch.Neo4jBatchGraph;


/**
 * An extended neo4j graph with additional features
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class ExtendedNeo4jBatchGraph extends Neo4jBatchGraph implements BenchmarkableGraph {	
	

	/**
	 * Create an instance of ExtendedNeo4jBatchGraph
	 * 
	 * @param rawGraph the graph
	 * @param indexProvider the index provider
	 */
	public ExtendedNeo4jBatchGraph(BatchInserter rawGraph,
			BatchInserterIndexProvider indexProvider) {
		super(rawGraph, indexProvider);
		init();
	}


	/**
	 * Create an instance of ExtendedNeo4jBatchGraph
	 * 
	 * @param directory the database directory
	 * @param parameters Neo4j configuration parameters
	 */
	public ExtendedNeo4jBatchGraph(String directory,
			Map<String, String> parameters) {
		super(directory, parameters);
		init();
	}


	/**
	 * Create an instance of ExtendedNeo4jBatchGraph
	 * 
	 * @param directory the database directory
	 */
	public ExtendedNeo4jBatchGraph(String directory) {
		super(directory);
		init();
	}

	
	/**
	 * Initialize the object
	 */
	private void init() {
	}

	
	/**
	 * Return the number of vertices
	 * 
	 * @return the number of vertices
	 */
    @Override
    public long countVertices() {
    	throw new UnsupportedOperationException();
    }
    
    
    /**
     * Return a random vertex
     * 
     * @return a random vertex
     */
    @Override
    public Vertex getRandomVertex() {
    	throw new UnsupportedOperationException();
    }
    
    
    /**
     * Return the number of edges
     * 
     * @return the number of edges
     */
    @Override
    public long countEdges() {
    	throw new UnsupportedOperationException();
    }
    
    
    /**
     * Return a random edge
     * 
     * @return a random edge
     */
    @Override
    public Edge getRandomEdge() {
    	throw new UnsupportedOperationException();
    }
    
    
    /**
     * Return the buffer pool size.
     * 
     * @return the buffer pool size in MB
     */
    @Override
    public int getBufferPoolSize() {
    	throw new UnsupportedOperationException();
    }
}
