package com.tinkerpop.blueprints.extensions.impls.neo4j;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.core.NodeManager;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.BenchmarkableGraph;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jEdge;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jVertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.KeyIndexableGraphHelper;


/**
 * An extended neo4j graph with additional features
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
@SuppressWarnings("deprecation")
public class ExtendedNeo4jGraph extends Neo4jGraph implements BenchmarkableGraph {
	
	protected NodeManager nodeManager;
	protected CacheProvider cacheProvider;
	
	
	/*
	 * Constructors created from the superclass
	 */

	public ExtendedNeo4jGraph(final GraphDatabaseService rawGraph, boolean fresh) {
		super(rawGraph, fresh);
		init();
	}

	public ExtendedNeo4jGraph(final GraphDatabaseService rawGraph) {
		super(rawGraph);
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
	 * Get the number of cache hits
	 * 
	 * @return the cache hits
	 */
	@Override
	public long getCacheHitCount() {
		long l = 0;
		for (org.neo4j.kernel.impl.cache.Cache<?> cache : nodeManager.caches()) {
			l += cache.hitCount();
		}
		return l;
	}
	
	
	/**
	 * Get the number of cache misses
	 * 
	 * @return the cache misses
	 */
	@Override
	public long getCacheMissCount() {
		long l = 0;
		for (org.neo4j.kernel.impl.cache.Cache<?> cache : nodeManager.caches()) {
			l += cache.missCount();
		}
		return l;
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
    
    
    /**
     * Return the total cache size, including the buffer pool and the session caches.
     * 
     * @return the cache size in MB
     */
    @Override
    public int getTotalCacheSize() {
    	throw new UnsupportedOperationException();
    }
    
    
    /**
     * Start a transaction (unless one is already running)
     */
    public void startTransaction() {
    	autoStartTransaction();
    }


    @Override
    public <T extends Element> void createKeyIndex(final String key, final Class<T> elementClass,
    		@SuppressWarnings("rawtypes") final Parameter... indexParameters) {

    	// Hack: a fixed version from the original Blueprints implementation, except without at least one of its bugs

    	this.autoStartTransaction();
    	GraphDatabaseService g = getRawGraph();
    	if (Vertex.class.isAssignableFrom(elementClass)) {
    		if (!g.index().getNodeAutoIndexer().isEnabled())
    			g.index().getNodeAutoIndexer().setEnabled(true);

    		g.index().getNodeAutoIndexer().startAutoIndexingProperty(key);
    		if (!this.getInternalIndexKeys(Vertex.class).contains(key)) {
    			KeyIndexableGraphHelper.reIndexElements(this, this.getVertices(), new HashSet<String>(Arrays.asList(key)));
    			this.createInternalIndexKey(key, elementClass);
    		}
    	} else if (Edge.class.isAssignableFrom(elementClass)) {
    		if (!g.index().getRelationshipAutoIndexer().isEnabled())
    			g.index().getRelationshipAutoIndexer().setEnabled(true);

    		g.index().getRelationshipAutoIndexer().startAutoIndexingProperty(key);
    		if (!this.getInternalIndexKeys(Edge.class).contains(key)) {
    			KeyIndexableGraphHelper.reIndexElements(this, this.getEdges(), new HashSet<String>(Arrays.asList(key)));
    			this.createInternalIndexKey(key, elementClass);
    		}
    	} else {
    		throw ExceptionFactory.classIsNotIndexable(elementClass);
    	}
    }

    
    private <T extends Element> void createInternalIndexKey(final String key, final Class<T> elementClass) {

    	// Hack: a fixed version from the original Blueprints implementation, except without at least one of its bugs

    	final String propertyName = elementClass.getSimpleName() + ":indexed_keys";
    	final PropertyContainer pc = ((GraphDatabaseAPI) getRawGraph()).getNodeManager().getGraphProperties();
    	this.autoStartTransaction();	// New
    	try {
    		final String[] keys = (String[]) pc.getProperty(propertyName);
    		final Set<String> temp = new HashSet<String>(Arrays.asList(keys));
    		temp.add(key);
    		pc.setProperty(propertyName, temp.toArray(new String[temp.size()]));
    	} catch (Exception e) {
    		// no indexed_keys kernel data property
    		pc.setProperty(propertyName, new String[]{key});
    	}
    }
}
