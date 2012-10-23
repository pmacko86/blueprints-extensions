package com.tinkerpop.blueprints.extensions.io.fgf;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.TransactionalGraph.Conclusion;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.BulkloadableGraph;
import com.tinkerpop.blueprints.extensions.io.GraphProgressListener;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.EdgeType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.PropertyType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.VertexType;
import com.tinkerpop.blueprints.impls.dex.DexGraph;
import com.tinkerpop.blueprints.impls.neo4jbatch.Neo4jBatchGraph;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;


/**
 * Fast Graph Format: Blueprints Graph loader
 * 
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class FGFGraphLoader {
	
	/**
	 * The key for the original FGF node ID
	 */
	public static final String KEY_ORIGINAL_ID = "_original_id";
	
	
	/**
	 * Load to an instance of a Graph
	 * 
	 * @param outGraph the graph to populate with the data
	 * @param file the input file
	 * @param bufferSize the transaction buffer size
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public static void loadTo(Graph outGraph, File file, int bufferSize) throws IOException, ClassNotFoundException {
		load(outGraph, file, bufferSize, false, true, null);
	}
	
	
	/**
	 * Load to an instance of a Graph and optionally index all property keys
	 * 
	 * @param outGraph the graph to populate with the data
	 * @param file the input file
	 * @param txBuffer the number of operations before a commit
	 * @param indexAllProperties whether to index all properties
	 * @param bulkLoad true to bulk-load the graph; false to use incremental load
	 * @param listener the progress listener
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public static void load(Graph outGraph, File file, int txBuffer,
			boolean indexAllProperties, boolean bulkLoad, GraphProgressListener listener)
					throws IOException, ClassNotFoundException {
		
		// Open the reader
		
		FGFReader reader = new FGFReader(file);
		
		
		// Check whether the input file and the settings are compatible with bulk-load, if it is enabled
		
		if (bulkLoad || outGraph instanceof Neo4jBatchGraph) {
			if (reader.getInitialVertexId() != 0) {
				reader.close();
				throw new IOException("The FGF file is not bulk-loadable: the initial vertex ID is not 0");
			}
			
			if (reader.getInitialEdgeId() != 0) {
				reader.close();
				throw new IOException("The FGF file is not bulk-loadable: the initial edge ID is not 0");
			}
		}
		
		
		// Initialize
		
		if (bulkLoad && outGraph instanceof BulkloadableGraph) {
			((BulkloadableGraph) outGraph).startBulkLoad();
		}

    	try {
    		
    		// Wrap the graph and start the loading process
    		
    		final Graph graph = bulkLoad && outGraph instanceof TransactionalGraph
    				? BatchGraph.wrap(outGraph, txBuffer) : outGraph;

    		Loader l = new Loader(graph, reader, txBuffer, indexAllProperties, listener);
    		reader.read(l);
    		l.finish();
    		l = null;
    		
    		
    		// Finish
    		
			if (listener != null) {
				listener.graphProgress((int) reader.getNumberOfVertices(),
						(int) reader.getNumberOfEdges());
			}
		}
    	finally {
    		
    		// Finalize
    		
    		if (bulkLoad && outGraph instanceof BulkloadableGraph) {
    			((BulkloadableGraph) outGraph).stopBulkLoad();
    		}
    	}
	}
	
	
	/**
	 * The actual graph loader
	 */
	private static class Loader implements FGFReaderHandler {
		
		private Graph graph;
		private FGFReader reader;
		private boolean indexAllProperties;
		private GraphProgressListener listener;
		
		@SuppressWarnings("unused")
		private Features features;
		private boolean supplyPropertiesAsIds;
		
		private Vertex[] vertices;
		private long vertexIdUpperBound;
		private Map<String, Object> tempMap;
		private long verticesLoaded;
		private long edgesLoaded;
		
		private int txBuffer;
		private int opsSinceCommit;
		private boolean txEnabled;
		
		private boolean alreadyHasVertexIdIndex;
		private boolean alreadyHasVertexLabelIndex;
		private boolean createdVertexIdIndex;
		private boolean createdVertexLabelIndex;
		private Set<String> preexistingVertexPropertyIndexes;
		private Set<String> preexistingEdgePropertyIndexes;
		
		
		/**
		 * Create an instance of class Loader
		 * @param graph the graph
		 * @param txBuffer the number of operations before a commit
		 * @param reader the input file reader
		 * @param indexAllProperties whether to index all properties
		 * @param listener the progress listener
		 */
		public Loader(Graph graph, FGFReader reader, int txBuffer, boolean indexAllProperties, GraphProgressListener listener) {
			
			this.graph = graph;
			this.reader = reader;
			this.txBuffer = txBuffer;
			this.indexAllProperties = indexAllProperties;
			this.listener = listener;
			
			this.features = graph.getFeatures();
			this.supplyPropertiesAsIds = graph instanceof Neo4jBatchGraph;
			
			this.vertexIdUpperBound = this.reader.getInitialVertexId() + this.reader.getNumberOfVertices(); 
			this.vertices = new Vertex[(int) this.vertexIdUpperBound];
			this.tempMap = new HashMap<String, Object>();
			this.verticesLoaded = 0;
			this.edgesLoaded = 0;
			this.opsSinceCommit = 0;
			
			this.alreadyHasVertexIdIndex = false;
			this.alreadyHasVertexLabelIndex = false;
			this.createdVertexIdIndex = false;
			this.createdVertexLabelIndex = false;
			
			if (graph instanceof KeyIndexableGraph) {
				KeyIndexableGraph g = (KeyIndexableGraph) graph;
				
				preexistingVertexPropertyIndexes = g.getIndexedKeys(Vertex.class);
				preexistingEdgePropertyIndexes   = g.getIndexedKeys(Edge.class);
				
				if (preexistingVertexPropertyIndexes.contains(KEY_ORIGINAL_ID)) {
					alreadyHasVertexIdIndex = true;
					createdVertexIdIndex    = true;
				}
				
				if (preexistingVertexPropertyIndexes.contains(KEY_ORIGINAL_ID)) {
					alreadyHasVertexLabelIndex = true;
					createdVertexLabelIndex    = true;
				}
			}
			else {
				preexistingVertexPropertyIndexes = new HashSet<String>();
				preexistingEdgePropertyIndexes   = new HashSet<String>();
			}
			
			this.txEnabled = graph instanceof TransactionalGraph
					&& !(graph instanceof BatchGraph) && !(graph instanceof Neo4jBatchGraph);
		}
		
		
		/**
		 * Finish loading
		 */
		public void finish() {
			if (txEnabled) {
				((TransactionalGraph) graph).stopTransaction(Conclusion.SUCCESS);
				opsSinceCommit = 0;
			}
		}
		

		/**
		 * Callback for a property type
		 * 
		 * @param type the property type object
		 */
		@Override
		public void propertyType(PropertyType type) {
			
			PropertyTypeAux aux = new PropertyTypeAux();
			type.setAux(aux);
			
			if (graph instanceof KeyIndexableGraph) {
				KeyIndexableGraph g = (KeyIndexableGraph) graph;
				Set<String> kv = g.getIndexedKeys(Vertex.class);
				if (kv.contains(type.getName())) aux.vertexIndexCreated = true;
			}
		}

		
		/**
		 * Callback for starting a new vertex type
		 * 
		 * @param type the vertex type
		 * @param count the number of vertices of the given type
		 */
		@Override
		public void vertexTypeStart(VertexType type, long count) {
			
			if (graph instanceof DexGraph) {
				((DexGraph) graph).label.set("".equals(type.getName()) ? DexGraph.DEFAULT_DEX_VERTEX_LABEL : type.getName());
				
				for (PropertyType t : reader.getPropertyTypes()) {
					((PropertyTypeAux) t.getAux()).vertexIndexCreated = preexistingVertexPropertyIndexes.contains(t.getName());
				}
				
				if (!alreadyHasVertexIdIndex   ) createdVertexIdIndex = false;
				if (!alreadyHasVertexLabelIndex) createdVertexLabelIndex = false;
			}
		}

		
		/**
		 * Callback for a vertex
		 * 
		 * @param id the vertex ID
		 * @param type the vertex type
		 * @param properties the map of properties
		 */
		@Override
		public void vertex(long id, VertexType type, Map<PropertyType, Object> properties) {
			
			Object a = id;
			boolean hasAdditionalLabel = false;
			if (supplyPropertiesAsIds) {
				tempMap.clear();
				for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
					tempMap.put(e.getKey().getName(), e.getValue());
				}
				if (!tempMap.containsKey(StringFactory.LABEL) && !"".equals(type.getName())) {
					tempMap.put(StringFactory.LABEL, type.getName());
					hasAdditionalLabel = true;
				}
				tempMap.put(KEY_ORIGINAL_ID, (int) id);
				a = tempMap;
			}
			
			
			// Create the vertex
			
			Vertex v = graph.addVertex(a);
			vertices[(int) id] = v;
			verticesLoaded++;
			opsSinceCommit++;
			
			
			// Properties
			
			if (!supplyPropertiesAsIds) {
				boolean hasLabel = false;
				for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
					hasLabel = hasLabel || StringFactory.LABEL.equals(e.getKey().getName()); 
					v.setProperty(e.getKey().getName(), e.getValue());
					opsSinceCommit++;
				}
				if (!hasLabel && !"".equals(type.getName())) {
					v.setProperty(StringFactory.LABEL, type.getName());
					hasAdditionalLabel = true;
					opsSinceCommit++;
				}
				v.setProperty(KEY_ORIGINAL_ID, (int) id);
				opsSinceCommit++;
			}
			
			if (graph instanceof KeyIndexableGraph) {
				if (!createdVertexIdIndex) {
					((KeyIndexableGraph) graph).createKeyIndex(KEY_ORIGINAL_ID, Vertex.class);
					createdVertexIdIndex = true;
				}
				
				if (hasAdditionalLabel && !createdVertexLabelIndex) {
					if (!(graph instanceof DexGraph)) {
						// In DEX, a vertex label index is implicit; please refer to
						// the implementation of DexGraph.getVertices(String, Object)
						// for more details.
						((KeyIndexableGraph) graph).createKeyIndex(StringFactory.LABEL, Vertex.class);
					}
					createdVertexLabelIndex = true;
				}
				
				if (indexAllProperties) {
					for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
						if (!((PropertyTypeAux) e.getKey().getAux()).vertexIndexCreated) {
							((KeyIndexableGraph) graph).createKeyIndex(e.getKey().getName(), Vertex.class);
							((PropertyTypeAux) e.getKey().getAux()).vertexIndexCreated = true;
						}
					}
				}
			}
			
			
			// Periodically commit
			
			if (txEnabled && opsSinceCommit > txBuffer) {
				((TransactionalGraph) graph).stopTransaction(Conclusion.SUCCESS);
				opsSinceCommit = 0;
			}
			
			
			// Listener callback
			
			if (listener != null && verticesLoaded % 10000 == 0) {
				listener.graphProgress((int) verticesLoaded, 0);
			}
		}

		
		/**
		 * Callback for starting the end of a vertex type
		 * 
		 * @param type the vertex type
		 * @param count the number of vertices of the given type
		 */
		@Override
		public void vertexTypeEnd(VertexType type, long count) {
			if (graph instanceof DexGraph) {
				((DexGraph) graph).label.set(null);
			}
		}

		
		/**
		 * Callback for starting a new edge type
		 * 
		 * @param type the edge type
		 * @param count the number of edges of the given type
		 */
		@Override
		public void edgeTypeStart(EdgeType type, long count) {
			if (graph instanceof DexGraph) {
				((DexGraph) graph).label.set(type.getName());
				
				for (PropertyType t : reader.getPropertyTypes()) {
					((PropertyTypeAux) t.getAux()).edgeIndexCreated = preexistingEdgePropertyIndexes.contains(t.getName());
				}
			}
		}


		/**
		 * Callback for an edge
		 * 
		 * @param id the edge ID
		 * @param head the vertex at the head
		 * @param tail the vertex at the tail
		 * @param type the edge type (label)
		 * @param properties the map of properties
		 */
		@Override
		public void edge(long id, long head, long tail, EdgeType type, Map<PropertyType, Object> properties) {
			
			Object a = id;
			if (supplyPropertiesAsIds) {
				tempMap.clear();
				for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
					tempMap.put(e.getKey().getName(), e.getValue());
				}
				a = tempMap;
			}
			
			
			// Look up the head and tail vertices; attempt to use the key indexes if the corresponding
			// Vertex objects are not readily available
			
			Vertex t = vertices[(int) tail];
			Vertex h = vertices[(int) head];
			
			if (t == null || h == null) {
				if (graph instanceof DexGraph) {
					((DexGraph) graph).label.set(null);
				}
				
				try {
					
					if (t == null) {
						Iterable<Vertex> i = graph.getVertices(KEY_ORIGINAL_ID, (int) tail);
						Iterator<Vertex> itr = i.iterator();
						if (itr.hasNext()) t = itr.next();
						boolean b = itr.hasNext();
						if (i instanceof CloseableIterable) ((CloseableIterable<?>) i).close();
						if (t == null) throw new RuntimeException("Cannot find vertex with " + KEY_ORIGINAL_ID + " " + tail);
						if (b) throw new RuntimeException("There is more than one vertex with " + KEY_ORIGINAL_ID + " " + tail);
					}
					
					if (h == null) {
						Iterable<Vertex> i = graph.getVertices(KEY_ORIGINAL_ID, (int) head);
						Iterator<Vertex> itr = i.iterator();
						if (itr.hasNext()) h = itr.next();
						boolean b = itr.hasNext();
						if (i instanceof CloseableIterable) ((CloseableIterable<?>) i).close();
						if (h == null) throw new RuntimeException("Cannot find vertex with " + KEY_ORIGINAL_ID + " " + head);
						if (b) throw new RuntimeException("There is more than one vertex with " + KEY_ORIGINAL_ID + " " + head);
					}
				}
				finally {
					if (graph instanceof DexGraph) {
						((DexGraph) graph).label.set(type.getName());
					}
				}
				
				if (graph instanceof DexGraph) {
					assert ((DexGraph) graph).label.get() != null;
				}
			}
			
			
			// Create the edge
			
			Edge e = graph.addEdge(a, t, h, type.getName());
			edgesLoaded++;
			opsSinceCommit++;
			
			
			// Set properties
			
			if (!supplyPropertiesAsIds) {
				for (Map.Entry<PropertyType, Object> p : properties.entrySet()) {
					e.setProperty(p.getKey().getName(), p.getValue());
					opsSinceCommit++;
				}
			}
			
			if (indexAllProperties && graph instanceof KeyIndexableGraph) {
				for (Map.Entry<PropertyType, Object> p : properties.entrySet()) {
					if (!((PropertyTypeAux) p.getKey().getAux()).edgeIndexCreated) {
						((KeyIndexableGraph) graph).createKeyIndex(p.getKey().getName(), Vertex.class);
						((PropertyTypeAux) p.getKey().getAux()).edgeIndexCreated = true;
					}
				}
			}
			
			
			// Periodically commit
			
			if (txEnabled && opsSinceCommit > txBuffer) {
				((TransactionalGraph) graph).stopTransaction(Conclusion.SUCCESS);
				opsSinceCommit = 0;
			}
			
			
			// Listener callback
		
			if (listener != null && edgesLoaded % 10000 == 0) {
				listener.graphProgress((int) verticesLoaded, (int) edgesLoaded);
			}
		}

		
		/**
		 * Callback for starting the end of an edge type
		 * 
		 * @param type the edge type
		 * @param count the number of edges of the given type
		 */
		@Override
		public void edgeTypeEnd(EdgeType type, long count) {
			if (graph instanceof DexGraph) {
				((DexGraph) graph).label.set(null);
			}
		}
		
		
		/**
		 * Additional property information
		 */
		private static class PropertyTypeAux {
			
			public boolean vertexIndexCreated = false;
			public boolean edgeIndexCreated = false;
			
			/**
			 * Create an instance of type PropertyTypeAux
			 */
			public PropertyTypeAux() {
				// Nothing to do
			}
		}
	}
}
