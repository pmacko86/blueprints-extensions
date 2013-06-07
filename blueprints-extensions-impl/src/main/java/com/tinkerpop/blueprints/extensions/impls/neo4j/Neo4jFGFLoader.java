package com.tinkerpop.blueprints.extensions.impls.neo4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.io.GraphProgressListener;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFConstants;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFFileReader.EdgeType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFFileReader.PropertyType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFFileReader;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFFileReader.VertexType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFFileReaderHandler;
import com.tinkerpop.blueprints.impls.neo4j.batch.Neo4jBatchGraph;
import com.tinkerpop.blueprints.util.StringFactory;


/**
 * Fast Graph Format: Neo4j Graph loader
 * 
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class Neo4jFGFLoader {
	
	
	/**
	 * Load from a FGF file
	 * 
	 * @param graph the batch graph
	 * @param file the input file
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public static void load(Neo4jBatchGraph graph, File file) throws IOException, ClassNotFoundException {
		load(graph, file, null);
	}	
	
	
	/**
	 * Load from a FGF file and optionally index all properties
	 * 
	 * @param graph the batch graph
	 * @param file the input file
	 * @param listener the progress listener
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public static void load(Neo4jBatchGraph graph, File file, GraphProgressListener listener)
			throws IOException, ClassNotFoundException {
		
		FGFFileReader reader = new FGFFileReader(file);
		
		if (reader.getInitialVertexId() != 0) {
			try {
				reader.close();
			}
			catch (Exception e) {};
			throw new IOException("The FGF file is not bulk-loadable: the initial vertex ID is not 0");
		}
		
		if (reader.getInitialEdgeId() != 0) {
			try {
				reader.close();
			}
			catch (Exception e) {};
			throw new IOException("The FGF file is not bulk-loadable: the initial edge ID is not 0");
		}

		Loader l = new Loader(graph, reader, false, listener);
		reader.read(l);
		l = null;
		
		if (listener != null) {
			listener.graphProgress((int) reader.getNumberOfVertices(),
					(int) reader.getNumberOfEdges());
		}
	}
	
	
	/**
	 * The actual graph loader
	 */
	private static class Loader implements FGFFileReaderHandler {
		
		private final Neo4jBatchGraph graph;
		private final BatchInserter inserter;
		private FGFFileReader reader;
		private boolean indexAllProperties;
		private GraphProgressListener listener;
		
		private long[] vertices;
		private Map<String, Object> tempMap;
		private DynamicRelationshipType relationshipType;
		private long verticesLoaded;
		private long edgesLoaded;
		private boolean hasAdditionalVertexLabel;
		private boolean additionalVertexLabelIndexCreated;
		private boolean originalVertexIdIndexCreated;
		
		
		/**
		 * Create an instance of class Loader
		 * 
		 * @param graph the graph
		 * @param reader the input file reader
		 * @param indexAllProperties whether to index all properties
		 * @param listener the progress listener
		 */
		public Loader(Neo4jBatchGraph graph, FGFFileReader reader, boolean indexAllProperties, GraphProgressListener listener) {
			
			this.graph = graph;
			this.inserter = graph.getRawGraph();
			this.reader = reader;
			this.indexAllProperties = indexAllProperties;
			this.listener = listener;
			
			this.vertices = new long[(int) this.reader.getNumberOfVertices()];
			this.tempMap = new HashMap<String, Object>();
			this.relationshipType = null;
			this.verticesLoaded = 0;
			this.edgesLoaded = 0;
			
			this.hasAdditionalVertexLabel = false;
			this.additionalVertexLabelIndexCreated = false;
			this.originalVertexIdIndexCreated = false;
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
		public void vertexTypeStart(VertexType type, long count) {
			// Nothing to do
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
			
			tempMap.clear();
			for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
				tempMap.put(e.getKey().getName(), e.getValue());
				((PropertyTypeAux) e.getKey().getAux()).vertexKeyUsed = true;
			}
			if (!tempMap.containsKey(StringFactory.LABEL) && !"".equals(type.getName())) {
				tempMap.put(StringFactory.LABEL, type.getName());
				hasAdditionalVertexLabel = true;
			}
			tempMap.put(FGFConstants.KEY_ORIGINAL_ID, (int) id);
			
			long v = inserter.createNode(tempMap);
			vertices[(int) id] = v;
			verticesLoaded++;
			
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
			
			if (!originalVertexIdIndexCreated) {
				graph.createKeyIndex(FGFConstants.KEY_ORIGINAL_ID, Vertex.class);
				originalVertexIdIndexCreated = true;
			}
			
			if (indexAllProperties && hasAdditionalVertexLabel && !additionalVertexLabelIndexCreated) {
				graph.createKeyIndex(StringFactory.LABEL, Vertex.class);
				additionalVertexLabelIndexCreated = true;
			}
			
			if (indexAllProperties) {
				for (PropertyType t : reader.getPropertyTypes()) {
					PropertyTypeAux x = (PropertyTypeAux) t.getAux();
					if (x.vertexKeyUsed && !x.vertexIndexCreated) {
						graph.createKeyIndex(t.getName(), Vertex.class);
						x.vertexIndexCreated = true;
					}
				}
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
			relationshipType = DynamicRelationshipType.withName(type.getName());
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
			
			tempMap.clear();
			for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
				tempMap.put(e.getKey().getName(), e.getValue());
				((PropertyTypeAux) e.getKey().getAux()).edgeKeyUsed = true;
			}
			
			inserter.createRelationship(vertices[(int) tail], vertices[(int) head], relationshipType, tempMap);
			edgesLoaded++;
			
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
			
			if (indexAllProperties) {
				for (PropertyType t : reader.getPropertyTypes()) {
					PropertyTypeAux x = (PropertyTypeAux) t.getAux();
					if (x.edgeKeyUsed && !x.edgeIndexCreated) {
						graph.createKeyIndex(t.getName(), Edge.class);
						x.edgeIndexCreated = true;
					}
				}
			}
		}
		
		
		/**
		 * Additional property information
		 */
		private static class PropertyTypeAux {
			
			public boolean vertexKeyUsed = false;
			public boolean vertexIndexCreated = false;
			public boolean edgeKeyUsed = false;
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
