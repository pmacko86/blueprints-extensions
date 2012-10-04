package com.tinkerpop.blueprints.extensions.impls.neo4j;

import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.core.NodeManager;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.BenchmarkableGraph;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jEdge;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jVertex;


/**
 * An extended neo4j graph with additional features
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class ExtendedNeo4jGraph extends Neo4jGraph implements BenchmarkableGraph {
	
	protected NodeManager nodeManager;
	protected CacheProvider cacheProvider;
	
	
	/*
	 * Constructors created from the superclass
	 */

	public ExtendedNeo4jGraph(GraphDatabaseService rawGraph, boolean fresh) {
		super(rawGraph, fresh);
		init();
	}

	public ExtendedNeo4jGraph(GraphDatabaseService rawGraph) {
		super(rawGraph);
		init();
	}

	public ExtendedNeo4jGraph(String directory, Map<String, String> configuration,
			boolean highAvailabilityMode) {
		super(directory, configuration, highAvailabilityMode);
		init();
	}

	public ExtendedNeo4jGraph(String directory,
			Map<String, String> configuration) {
		super(directory, configuration);
		init();
	}

	public ExtendedNeo4jGraph(String directory) {
		super(directory);
		init();
	}

	
	/**
	 * Initialize the object
	 */
	private void init() {
		
		GraphDatabaseService rawGraph = getRawGraph();
        if (rawGraph instanceof HighlyAvailableGraphDatabase) {
        	nodeManager = ((HighlyAvailableGraphDatabase) rawGraph).getNodeManager();
        }
        else if (rawGraph instanceof EmbeddedGraphDatabase) {
        	nodeManager = ((EmbeddedGraphDatabase) rawGraph).getNodeManager();
        }
        else {
        	throw new IllegalStateException("Unrecognized type of neo4j GraphDatabaseService");
        }
        
    	cacheProvider = nodeManager.getCacheType();
	}
	
	
	/**
	 * Get the name of the cache provider
	 * 
	 * @return the name of the cache provider
	 */
	public String getCacheProviderName() {
		return cacheProvider.getName();
	}

	
	/**
	 * Return the number of vertices
	 * 
	 * @return the number of vertices
	 */
    @Override
    public long countVertices() {
    	return nodeManager.getNumberOfIdsInUse(Node.class);
    }
    
    
    /**
     * Return a random vertex
     * 
     * @return a random vertex
     */
    @Override
    public Vertex getRandomVertex() {
    	
    	// From: http://grepcode.com/file/repo1.maven.org/maven2/org.neo4j/neo4j-kernel/1.5.M02/org/neo4j/test/RandomNode.java/
    	
    	Node node = null;
    	long totalVertices = -1;
        do {
            try {
            	node = getRawGraph().getNodeById((long)(Math.random()
            			* (nodeManager.getHighestPossibleIdInUse(Node.class) + 1)));
            }
            catch (NotFoundException loop) {
            	if (totalVertices < 0) {
            		totalVertices = countVertices();
            		if (totalVertices == 0) {
            			throw new NoSuchElementException();
            		}
            	}
            	continue;
            }
        }
        while (node == null);
        
        return new Neo4jVertex(node, this);
    }
    
    
    /**
     * Return the number of edges
     * 
     * @return the number of edges
     */
    @Override
    public long countEdges() {
    	return nodeManager.getNumberOfIdsInUse(Relationship.class);
    }
    
    
    /**
     * Return a random edge
     * 
     * @return a random edge
     */
    @Override
    public Edge getRandomEdge() {
    	
    	Relationship rel = null;
    	long totalEdges = -1;
        do {
            try {
            	rel = getRawGraph().getRelationshipById((long)(Math.random()
            			* (nodeManager.getHighestPossibleIdInUse(Relationship.class) + 1)));
            }
            catch (NotFoundException loop) {
            	if (totalEdges < 0) {
            		totalEdges = countEdges();
            		if (totalEdges == 0) {
            			throw new NoSuchElementException();
            		}
            	}
            	continue;
            }
        }
        while (rel == null);
        
        return new Neo4jEdge(rel, this);
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
