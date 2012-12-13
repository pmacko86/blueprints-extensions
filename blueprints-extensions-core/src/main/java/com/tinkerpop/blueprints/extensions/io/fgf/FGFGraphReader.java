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
import com.tinkerpop.blueprints.extensions.io.GraphProgressListener;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFFileReader.EdgeType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFFileReader.PropertyType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFFileReader.VertexType;
import com.tinkerpop.blueprints.impls.dex.DexGraph;
import com.tinkerpop.blueprints.impls.neo4jbatch.Neo4jBatchGraph;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;


/**
 * Fast Graph Format: Blueprints Graph Reader
 * 
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class FGFGraphReader {
	
	private final Graph graph;
	private boolean bulkLoad = true;
	private boolean createOriginalIdProperty = false;
	private GraphProgressListener listener = null;
	
	
	/**
	 * Create an instance of class FGFGraphReader
	 * 
	 * @param graph the graph to populate with the data
	 */
	public FGFGraphReader(final Graph graph) {
		this.graph = graph;
	}
	
	
	/**
	 * Set whether to use the bulk-load settings when loading the graph. Use
	 * this only if the supplied instance of Graph is empty, and the FGF file's
	 * initial vertex and edge IDs are both zero.
	 * 
	 * @param bulkLoad true to bulk-load the graph; false to use incremental load
	 */
	public void setBulkLoad(final boolean bulkLoad) {
		this.bulkLoad = bulkLoad;
	}
	
	
	/**
	 * Set whether to create the FGFConstants.KEY_ORIGINAL_ID property ("_original_id")
	 * which contains the original ID of each vertex in its FGF file. This is required
	 * to incrementally load a graph using a FGF file.
	 * 
	 * @param createOriginalIdProperty true to create the FGFConstants.KEY_ORIGINAL_ID property
	 */
	public void setCreateOriginalIdProperty(final boolean createOriginalIdProperty) {
		this.createOriginalIdProperty = createOriginalIdProperty;
	}
	
	
	/**
	 * Load to the instance of Graph
	 * 
	 * @param file the input file
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public void inputGraph(final File file)
					 throws IOException, ClassNotFoundException {
		inputGraph(graph, file, 1000, bulkLoad, createOriginalIdProperty, listener);
	}
	
	
	/**
	 * Load to the instance of Graph
	 * 
	 * @param file the input file
	 * @param txBuffer the transaction buffer size
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public void inputGraph(final File file, final int txBuffer)
					 throws IOException, ClassNotFoundException {
		inputGraph(graph, file, txBuffer, bulkLoad, createOriginalIdProperty, listener);
	}
	
	
	/**
	 * Load to an instance of Graph
	 * 
	 * @param graph the graph to populate with the data
	 * @param file the input file
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public static void inputGraph(final Graph graph, final File file)
					 throws IOException, ClassNotFoundException {
		inputGraph(graph, file, 1000, true, false, null);
	}
	
	
	/**
	 * Load to an instance of Graph
	 * 
	 * @param graph the graph to populate with the data
	 * @param file the input file
	 * @param txBuffer the transaction buffer size
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public static void inputGraph(final Graph graph, final File file,
			 final int txBuffer)
					 throws IOException, ClassNotFoundException {
		inputGraph(graph, file, txBuffer, true, false, null);
	}
	
	
	/**
	 * Load to an instance of Graph and optionally index all property keys
	 * 
	 * @param graph the graph to populate with the data
	 * @param file the input file
	 * @param txBuffer the number of operations before a commit
	 * @param bulkLoad true to bulk-load the graph; false to use incremental load
	 * @param createOriginalIdProperty true to create the FGFConstants.KEY_ORIGINAL_ID property
	 * @param listener the progress listener
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public static void inputGraph(final Graph graph, final File file, final int txBuffer,
			final boolean bulkLoad, final boolean createOriginalIdProperty,
			final GraphProgressListener listener)
					throws IOException, ClassNotFoundException {
		
		// Open the reader
		
		FGFFileReader reader = new FGFFileReader(file);
		
		
		// Check whether the input file and the settings are compatible with bulk-load, if it is enabled
		
		if (bulkLoad || graph instanceof Neo4jBatchGraph) {
			if (reader.getInitialVertexId() != 0) {
				reader.close();
				throw new IOException("The FGF file is not bulk-loadable: the initial vertex ID is not 0");
			}
			
			if (reader.getInitialEdgeId() != 0) {
				reader.close();
				throw new IOException("The FGF file is not bulk-loadable: the initial edge ID is not 0");
			}
		}
		
		if (!bulkLoad && !createOriginalIdProperty) {
			reader.close();
			throw new IOException("The use of the " + FGFConstants.KEY_ORIGINAL_ID + " property is required for incremental load");
		}
		
		
		// Wrap the graph and start the loading process
		
		final Graph wrappedGraph = bulkLoad && graph instanceof TransactionalGraph
				? BatchGraph.wrap(graph, txBuffer) : graph;

		Loader l = new Loader(wrappedGraph, reader, txBuffer, false /* do not index all properties */,
				createOriginalIdProperty, listener);
		reader.read(l);
		l.finish();
		l = null;
		
		
		// Finish
		
		if (listener != null) {
			listener.graphProgress((int) reader.getNumberOfVertices(),
					(int) reader.getNumberOfEdges());
		}
	}
	
	
	/**
	 * The actual graph loader
	 */
	private static class Loader implements FGFFileReaderHandler {
		
		private Graph graph;
		private FGFFileReader reader;
		private boolean indexAllProperties;
		private boolean createOriginalIdProperty;
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
		 * 
		 * @param graph the graph
		 * @param txBuffer the number of operations before a commit
		 * @param reader the input file reader
		 * @param indexAllProperties whether to index all properties
		 * @param createOriginalIdProperty true to create the KEY_ORIGINAL_ID property
		 * @param listener the progress listener
		 */
		public Loader(Graph graph, FGFFileReader reader, int txBuffer,
				boolean indexAllProperties, boolean createOriginalIdProperty,
				GraphProgressListener listener) {
			
			this.graph = graph;
			this.reader = reader;
			this.txBuffer = txBuffer;
			this.indexAllProperties = indexAllProperties;
			this.createOriginalIdProperty = createOriginalIdProperty;
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
				
				if (preexistingVertexPropertyIndexes.contains(FGFConstants.KEY_ORIGINAL_ID)) {
					alreadyHasVertexIdIndex = true;
					createdVertexIdIndex    = true;
				}
				
				if (preexistingVertexPropertyIndexes.contains(FGFConstants.KEY_ORIGINAL_ID)) {
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
				((DexGraph) graph).label.set(FGFFileWriter.DEFAULT_VERTEX_TYPE.equals(type.getName())
						? DexGraph.DEFAULT_DEX_VERTEX_LABEL : type.getName());
				
				for (PropertyType t : reader.getPropertyTypes()) {
					((PropertyTypeAux) t.getAux()).vertexIndexCreated
						= preexistingVertexPropertyIndexes.contains(t.getName());
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
				if (createOriginalIdProperty) tempMap.put(FGFConstants.KEY_ORIGINAL_ID, (int) id);
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
				if (createOriginalIdProperty) v.setProperty(FGFConstants.KEY_ORIGINAL_ID, (int) id);
				opsSinceCommit++;
			}
			
			if (graph instanceof KeyIndexableGraph) {
				if (!createdVertexIdIndex && createOriginalIdProperty) {
					((KeyIndexableGraph) graph).createKeyIndex(FGFConstants.KEY_ORIGINAL_ID, Vertex.class);
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
		 * @param tail the tail vertex id (also known as the "out" or the "source" vertex)
		 * @param head the head vertex id (also known as the "in" or the "target" vertex)
		 * @param type the edge type (label)
		 * @param properties the map of properties
		 */
		@Override
		public void edge(long id, long tail, long head, EdgeType type, Map<PropertyType, Object> properties) {
			
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
						Iterable<Vertex> i = graph.getVertices(FGFConstants.KEY_ORIGINAL_ID, (int) tail);
						Iterator<Vertex> itr = i.iterator();
						if (itr.hasNext()) t = itr.next();
						boolean b = itr.hasNext();
						if (i instanceof CloseableIterable) ((CloseableIterable<?>) i).close();
						if (t == null) throw new RuntimeException("Cannot find vertex with " + FGFConstants.KEY_ORIGINAL_ID + " " + tail);
						if (b) throw new RuntimeException("There is more than one vertex with " + FGFConstants.KEY_ORIGINAL_ID + " " + tail);
						vertices[(int) tail] = t;
					}
					
					if (h == null) {
						Iterable<Vertex> i = graph.getVertices(FGFConstants.KEY_ORIGINAL_ID, (int) head);
						Iterator<Vertex> itr = i.iterator();
						if (itr.hasNext()) h = itr.next();
						boolean b = itr.hasNext();
						if (i instanceof CloseableIterable) ((CloseableIterable<?>) i).close();
						if (h == null) throw new RuntimeException("Cannot find vertex with " + FGFConstants.KEY_ORIGINAL_ID + " " + head);
						if (b) throw new RuntimeException("There is more than one vertex with " + FGFConstants.KEY_ORIGINAL_ID + " " + head);
						vertices[(int) head] = h;
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
