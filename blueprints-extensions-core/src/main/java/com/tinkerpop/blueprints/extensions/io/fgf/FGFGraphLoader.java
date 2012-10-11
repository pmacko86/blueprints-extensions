package com.tinkerpop.blueprints.extensions.io.fgf;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.BulkloadableGraph;
import com.tinkerpop.blueprints.extensions.io.GraphProgressListener;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.PropertyType;
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
		load(outGraph, file, bufferSize, false, null);
	}
	
	
	/**
	 * Load to an instance of a Graph and optionally index all property keys
	 * 
	 * @param outGraph the graph to populate with the data
	 * @param file the input file
	 * @param bufferSize the transaction buffer size
	 * @param indexAllProperties whether to index all properties
	 * @param listener the progress listener
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public static void load(Graph outGraph, File file, int bufferSize,
			boolean indexAllProperties, GraphProgressListener listener) throws IOException, ClassNotFoundException {
		
		FGFReader reader = new FGFReader(file);

		if (outGraph instanceof BulkloadableGraph) {
			((BulkloadableGraph) outGraph).startBulkLoad();
		}

    	try {
    		final Graph graph = outGraph instanceof TransactionalGraph
    				? BatchGraph.wrap(outGraph, bufferSize) : outGraph;

    		Loader l = new Loader(graph, reader, indexAllProperties, listener);
    		reader.read(l);
    		l = null;
    		
			if (listener != null) {
				listener.graphProgress((int) reader.getNumberOfVertices(),
						(int) reader.getNumberOfEdges());
			}
		}
    	finally {
    		if (outGraph instanceof BulkloadableGraph) {
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
		private Map<String, Object> tempMap;
		private long verticesLoaded;
		private long edgesLoaded;
		private boolean createdVertexIdIndex;
		private boolean createdVertexLabelIndex;
		
		
		/**
		 * Create an instance of class Loader
		 * @param graph the graph
		 * @param reader the input file reader
		 * @param indexAllProperties whether to index all properties
		 * @param listener the progress listener
		 */
		public Loader(Graph graph, FGFReader reader, boolean indexAllProperties, GraphProgressListener listener) {
			
			this.graph = graph;
			this.reader = reader;
			this.indexAllProperties = indexAllProperties;
			this.listener = listener;
			
			this.features = graph.getFeatures();
			this.supplyPropertiesAsIds = graph instanceof Neo4jBatchGraph;
			
			this.vertices = new Vertex[(int) this.reader.getNumberOfVertices()];
			this.tempMap = new HashMap<String, Object>();
			this.verticesLoaded = 0;
			this.edgesLoaded = 0;
			this.createdVertexIdIndex = false;
			this.createdVertexLabelIndex = false;
		}
		

		/**
		 * Callback for a property type
		 * 
		 * @param type the property type object
		 */
		@Override
		public void propertyType(PropertyType type) {
			type.setAux(new PropertyTypeAux());
		}

		
		/**
		 * Callback for starting a new vertex type
		 * 
		 * @param type the vertex type
		 * @param count the number of vertices of the given type
		 */
		@Override
		public void vertexTypeStart(String type, long count) {
			
			if (graph instanceof DexGraph) {
				((DexGraph) graph).label.set("".equals(type) ? DexGraph.DEFAULT_DEX_VERTEX_LABEL : type);
				
				for (PropertyType t : reader.getPropertyTypes()) {
					((PropertyTypeAux) t.getAux()).vertexIndexCreated = false;
				}
				
				createdVertexIdIndex = false;
				createdVertexLabelIndex = false;
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
		public void vertex(long id, String type, Map<PropertyType, Object> properties) {
			
			Object a = id;
			boolean hasAdditionalLabel = false;
			if (supplyPropertiesAsIds) {
				tempMap.clear();
				for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
					tempMap.put(e.getKey().getName(), e.getValue());
				}
				if (!tempMap.containsKey(StringFactory.LABEL) && !"".equals(type)) {
					tempMap.put(StringFactory.LABEL, type);
					hasAdditionalLabel = true;
				}
				tempMap.put(KEY_ORIGINAL_ID, (int) id);
				a = tempMap;
			}
			
			Vertex v = graph.addVertex(a);
			vertices[(int) id] = v;
			verticesLoaded++;
			
			if (!supplyPropertiesAsIds) {
				boolean hasLabel = false;
				for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
					hasLabel = hasLabel || StringFactory.LABEL.equals(e.getKey().getName()); 
					v.setProperty(e.getKey().getName(), e.getValue());
				}
				if (!hasLabel && !"".equals(type)) {
					v.setProperty(StringFactory.LABEL, type);
					hasAdditionalLabel = true;
				}
				v.setProperty(KEY_ORIGINAL_ID, (int) id);
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
		public void vertexTypeEnd(String type, long count) {
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
		public void edgeTypeStart(String type, long count) {
			if (graph instanceof DexGraph) {
				((DexGraph) graph).label.set(type);
				
				for (PropertyType t : reader.getPropertyTypes()) {
					((PropertyTypeAux) t.getAux()).edgeIndexCreated = false;
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
		public void edge(long id, long head, long tail, String type, Map<PropertyType, Object> properties) {
			
			Object a = id;
			if (supplyPropertiesAsIds) {
				tempMap.clear();
				for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
					tempMap.put(e.getKey().getName(), e.getValue());
				}
				a = tempMap;
			}
			
			Edge e = graph.addEdge(a, vertices[(int) tail], vertices[(int) head], type);
			edgesLoaded++;
			
			if (!supplyPropertiesAsIds) {
				for (Map.Entry<PropertyType, Object> p : properties.entrySet()) {
					e.setProperty(p.getKey().getName(), p.getValue());
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
		public void edgeTypeEnd(String type, long count) {
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
