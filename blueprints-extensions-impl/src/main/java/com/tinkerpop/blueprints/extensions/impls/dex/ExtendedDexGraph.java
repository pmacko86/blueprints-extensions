package com.tinkerpop.blueprints.extensions.impls.dex;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.NoSuchElementException;

import com.sparsity.dex.gdb.Attribute;
import com.sparsity.dex.gdb.AttributeList;
import com.sparsity.dex.gdb.DexProperties;
import com.sparsity.dex.gdb.Graph;
import com.sparsity.dex.gdb.ObjectType;
import com.sparsity.dex.gdb.Session;
import com.sparsity.dex.gdb.StringList;
import com.sparsity.dex.gdb.Type;
import com.sparsity.dex.gdb.TypeList;
import com.sparsity.dex.io.CSVWriter;
import com.sparsity.dex.io.EdgeTypeExporter;
import com.sparsity.dex.io.NodeTypeExporter;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.BenchmarkableGraph;
import com.tinkerpop.blueprints.impls.dex.DexGraph;


/**
 * An extended DEX graph with additional features
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class ExtendedDexGraph extends DexGraph implements BenchmarkableGraph {
	
	/**
	 * The step in which DEX partitions its ID space
	 */
	public static final long ID_STEP = 1024;
	
	
	/// The session
	private Session session;
	

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
		try {
			Method m = DexGraph.class.getDeclaredMethod("getRawSession");
			m.setAccessible(true);
			session = (Session) m.invoke(this);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	
	/**
	 * Return the session
	 * 
	 * @return the session
	 */
	public Session getSession() {
		return session;
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
    	
    	throw new IllegalStateException("Cannot determine the persitent pool size.");
    }
    
    
    /**
     * Return the total cache size, including the buffer pool and the session caches.
     * 
     * @return the cache size in MB
     */
    @Override
    public int getTotalCacheSize() {
    	
    	int maxCache = DexProperties.getInteger("dex.io.cache.maxsize", -1);
    	if (maxCache >= 0) return maxCache;
    	
    	throw new IllegalStateException("Cannot determine the cache size.");
    }
	
    
	/**
	 * Get the number of cache hits
	 * 
	 * @return the cache hits
	 */
    @Override
	public long getCacheHitCount() {
    	return -1;	// Not available
    }

    
	/**
	 * Get the number of cache misses
	 * 
	 * @return the cache misses
	 */
    @Override
	public long getCacheMissCount() {
    	return -1;	// Not available
    }

    
    /**
     * Export a type - a helper to exportToCSVs()
     * 
     * @param dir the directory into which the files should be placed
     * @param prefix the file name prefix
     * @param g the graph
     * @param type the type
     * @param nodes true if it these are nodes
     * @throws IOException on I/O error
     */
    private void exportTypeToCSV(File dir, String prefix, Graph g, int type, boolean nodes) throws IOException {
    	
		// Get and encode the type name
		
		String typeName = g.getType(type).getName();
		String encodedTypeName = "";
		for (int i = 0; i < typeName.length(); i++) {
			char c = typeName.charAt(i);
			if (Character.isLetterOrDigit(c) || c == '_') {
				encodedTypeName += c;
			}
			else {
				encodedTypeName += "-" + Integer.toHexString(((int) c) < 0 ? -((int) c) : (int) c);
			}
		}
		
		
		// Get the list of attributes
		
		AttributeList alist = g.findAttributes(type);
		StringList aNameList = new StringList();
		for (Integer aindex : alist) {
			Attribute a = g.getAttribute(aindex);
			aNameList.add(a.getName());
		}
		
		
		// Export
		
		String fileName = prefix + "-" + (nodes ? "nodes" : "edges") + encodedTypeName + ".csv";
		File f = new File(dir, fileName);
		
		CSVWriter out = new CSVWriter();
		out.open(f.getAbsolutePath());
		out.write(aNameList);
		
		if (nodes) {
			NodeTypeExporter exporter = new NodeTypeExporter(out, g, type, alist);
			exporter.run();
		}
		else {
			// XXX Not working
			EdgeTypeExporter exporter = new EdgeTypeExporter(out, g, type, alist, 0, 1, 2, 2);
			exporter.run();
		}
		
		out.close();
    }
    
    
    /**
     * Export to a set of .csv files
     * 
     * @param dir the directory into which the files should be placed
     * @param prefix the file name prefix
     * @throws IOException on I/O error
     */
    public void exportToCSVs(File dir, String prefix) throws IOException {
    	
    	Graph g = getRawGraph();
    	dir.mkdirs();
    	
    	
    	// For each vertex type...

    	TypeList tlist = g.findNodeTypes();
    	for (Integer type : tlist) {
    		exportTypeToCSV(dir, prefix, g, type, true);
    	}
    	tlist.delete();


    	// For each edge type...

    	tlist = g.findEdgeTypes();
    	for (Integer type : tlist) {
    		exportTypeToCSV(dir, prefix, g, type, false);
    	}
    	tlist.delete();
    }
}
