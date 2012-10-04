package com.tinkerpop.blueprints.extensions.impls.dex;

import java.util.NoSuchElementException;

import com.sparsity.dex.gdb.DexProperties;
import com.sparsity.dex.gdb.Graph;
import com.sparsity.dex.gdb.ObjectType;
import com.sparsity.dex.gdb.Type;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.BenchmarkableGraph;
import com.tinkerpop.blueprints.impls.dex.DexGraph;



/**
 * An extended neo4j graph with additional features
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class ExtendedDexGraph extends DexGraph implements BenchmarkableGraph {
	
	/**
	 * The step in which DEX partitions its ID space
	 */
	public static final long ID_STEP = 1024;
	

	/**
	 * Create an instance of ExtendedDexGraph
	 * 
	 * @param fileName the file name
	 */
	public ExtendedDexGraph(String fileName) {
		super(fileName);
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
    	return getRawGraph().countNodes();
    }
    
    
    /**
     * Return a random vertex
     * 
     * @return a random vertex, or null if not found
     */
    @Override
    public Vertex getRandomVertex() {
    	
    	Graph rawGraph = getRawGraph();
    	
    	long nodes = rawGraph.countNodes();
    	long edges = rawGraph.countEdges();
    	
    	if (nodes <= 0) throw new NoSuchElementException();
    	
    	
    	// Get the minimum and the maximum Id
    	
    	long nodes_up = (nodes / ID_STEP) * ID_STEP;
    	long edges_up = (edges / ID_STEP) * ID_STEP;
    	if (nodes_up != nodes) nodes_up += ID_STEP;
    	if (edges_up != edges) edges_up += ID_STEP;
    	
    	long minId = ID_STEP;
    	long maxId = minId + Math.max(nodes_up + edges, nodes + edges_up);
    	
    	
    	// Pick a random item
    	
    	while (true) {
    		long item = minId + (long)(Math.random() * (maxId - minId));
    		try {
    			int type = rawGraph.getObjectType(item);
    			if (type == Type.InvalidType) continue;
    			if (rawGraph.getType(type).getObjectType() == ObjectType.Node) {
    				return getVertex(item);
    			}
    		}
    		catch (RuntimeException e) {
    			// DEX throws an runtime exception => [DEX: 12] Invalid object identifier.
    			continue;
    		}
    	}
    }
    
    
    /**
     * Return the number of edges
     * 
     * @return the number of edges
     */
    @Override
    public long countEdges() {
    	return getRawGraph().countEdges();
    }
    
    
    /**
     * Return a random edge
     * 
     * @return a random edge
     */
    @Override
    public Edge getRandomEdge() {
    	
    	Graph rawGraph = getRawGraph();
    	
    	long nodes = rawGraph.countNodes();
    	long edges = rawGraph.countEdges();
    	
    	if (edges <= 0) throw new NoSuchElementException();
    	
    	
    	// Get the minimum and the maximum Id
    	
    	long nodes_up = (nodes / ID_STEP) * ID_STEP;
    	long edges_up = (edges / ID_STEP) * ID_STEP;
    	if (nodes_up != nodes) nodes_up += ID_STEP;
    	if (edges_up != edges) edges_up += ID_STEP;
    	
    	long minId = ID_STEP;
    	long maxId = minId + Math.max(nodes_up + edges, nodes + edges_up);
    	
    	
    	// Pick a random item
    	
    	while (true) {
    		long item = minId + (long)(Math.random() * (maxId - minId));
    		try {
    			int type = rawGraph.getObjectType(item);
    			if (type == Type.InvalidType) continue;
    			if (rawGraph.getType(type).getObjectType() == ObjectType.Edge) {
    				return getEdge(item);
    			}
    		}
    		catch (RuntimeException e) {
    			// DEX throws an runtime exception => [DEX: 12] Invalid object identifier.
    			continue;
    		}
    	}
    }  
    
    
    /**
     * Return the buffer pool size.
     * 
     * @return the buffer pool size in MB
     */
    @Override
    public int getBufferPoolSize() {
    	
    	int frameSize = DexProperties.getInteger("dex.io.pool.frame.size", 1)
    			* DexProperties.getInteger("dex.storage.extentsize", 4 /* KB */);
    	int maxPersistentPool = DexProperties.getInteger("dex.io.pool.persistent.maxsize", 0);
    	if (maxPersistentPool > 0 && frameSize > 0) {
    		return maxPersistentPool * frameSize / 1024;
    	}
    	
    	int maxCache = DexProperties.getInteger("dex.io.cache.maxsize", -1);
    	if (maxCache >= 0) return maxCache - 64;	// XXX This is horrible hack!
    	
    	throw new IllegalStateException("Cannot determine the persitent pool size.");
    }
}
