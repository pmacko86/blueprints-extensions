package com.tinkerpop.blueprints.extensions.impls.neo4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;

import com.tinkerpop.blueprints.extensions.io.GraphProgressListener;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.PropertyType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReaderHandler;


/**
 * Fast Graph Format: Neo4j Graph loader
 * 
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class Neo4jFGFLoader {
	
	
	/**
	 * Load from a FGF file
	 * 
	 * @param inserter the batch inserter
	 * @param file the input file
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public static void load(BatchInserter inserter, File file) throws IOException, ClassNotFoundException {
		load(inserter, file, null);
	}	
	
	
	/**
	 * Load from a FGF file
	 * 
	 * @param inserter the batch inserter
	 * @param listener the progress listener
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public static void load(BatchInserter inserter, File file, GraphProgressListener listener)
			throws IOException, ClassNotFoundException {
		
		FGFReader reader = new FGFReader(file);

		Loader l = new Loader(inserter, reader, listener);
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
	private static class Loader implements FGFReaderHandler {
		
		private final BatchInserter graph;
		private FGFReader reader;
		private GraphProgressListener listener;
		
		private long[] vertices;
		private Map<String, Object> tempMap;
		private DynamicRelationshipType relationshipType;
		private long verticesLoaded;
		private long edgesLoaded;
		
		
		/**
		 * Create an instance of class Loader
		 * 
		 * @param graph the graph
		 * @param reader the input file reader
		 * @param listener the progress listener
		 */
		public Loader(BatchInserter graph, FGFReader reader, GraphProgressListener listener) {
			
			this.graph = graph;
			this.reader = reader;
			this.listener = listener;
			
			this.vertices = new long[(int) this.reader.getNumberOfVertices()];
			this.tempMap = new HashMap<String, Object>();
			this.relationshipType = null;
			this.verticesLoaded = 0;
			this.edgesLoaded = 0;
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
			
			tempMap.clear();
			for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
				tempMap.put(e.getKey().getName(), e.getValue());
			}
			if (!tempMap.containsKey("type") && !"".equals(type)) {
				tempMap.put("type", type);
			}
			
			long v = graph.createNode(tempMap);
			vertices[(int) id] = v;
			verticesLoaded++;
			
			if (listener != null && verticesLoaded % 10000 == 0) {
				listener.graphProgress((int) verticesLoaded, 0);
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
			relationshipType = DynamicRelationshipType.withName(type);
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
			
			tempMap.clear();
			for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
				tempMap.put(e.getKey().getName(), e.getValue());
			}
			
			graph.createRelationship(vertices[(int) head], vertices[(int) tail], relationshipType, tempMap);
			edgesLoaded++;
			
			if (listener != null && edgesLoaded % 10000 == 0) {
				listener.graphProgress((int) verticesLoaded, (int) edgesLoaded);
			}
		}

		
		public void propertyType(PropertyType type) {}
		public void vertexTypeStart(String type, long count) {}
		public void vertexTypeEnd(String type, long count) {}
		public void edgeTypeEnd(String type, long count) {}
	}
}
