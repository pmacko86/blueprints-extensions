package com.tinkerpop.blueprints.extensions.io.fgf;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.BulkloadableGraph;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.PropertyType;
import com.tinkerpop.blueprints.impls.neo4jbatch.Neo4jBatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;


/**
 * Fast Graph Format: Blueprints Graph loader
 * 
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class FGFGraphLoader {

	private File file;
	
	
	/**
	 * Create a new instance of the class FGFGraphLoader
	 * 
	 * @param file the input file
	 */
	public FGFGraphLoader(File file) {
		this.file = file;
	}
	
	
	/**
	 * Load to an instance of a Graph
	 * 
	 * @param outGraph the graph to populate with the data
	 * @param bufferSize the transaction buffer size
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public void loadTo(Graph outGraph, int bufferSize) throws IOException, ClassNotFoundException {
		
		FGFReader reader = new FGFReader(file);

		if (outGraph instanceof BulkloadableGraph) {
			((BulkloadableGraph) outGraph).startBulkLoad();
		}

    	try {
    		final Graph graph = outGraph instanceof TransactionalGraph
    				? BatchGraph.wrap(outGraph, bufferSize)
    				: outGraph;

    		Loader l = new Loader(graph, reader);
    		reader.read(l);
    		l = null;
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
	private class Loader implements FGFReaderHandler {
		
		private Graph graph;
		private FGFReader reader;
		
		@SuppressWarnings("unused")
		private Features features;
		private boolean supplyPropertiesAsIds;
		
		private Vertex[] vertices;
		private Map<String, Object> tempMap;
		
		
		/**
		 * Create an instance of class Loader
		 * @param graph the graph
		 * @param reader the input file reader
		 */
		public Loader(Graph graph, FGFReader reader) {
			
			this.graph = graph;
			this.reader = reader;
			
			this.features = graph.getFeatures();
			this.supplyPropertiesAsIds = graph instanceof Neo4jBatchGraph;
			
			this.vertices = new Vertex[(int) this.reader.getNumberOfVertices()];
			this.tempMap = new HashMap<String, Object>();
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
			if (supplyPropertiesAsIds) {
				tempMap.clear();
				for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
					tempMap.put(e.getKey().getName(), e.getValue());
				}
				if (!tempMap.containsKey("type") && !"".equals(type)) {
					tempMap.put("type", type);
				}
				a = tempMap;
			}
			
			Vertex v = graph.addVertex(a);
			vertices[(int) id] = v;
			
			if (!supplyPropertiesAsIds) {
				boolean hasType = false;
				for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
					hasType = hasType || "type".equals(e.getKey().getName()); 
					v.setProperty(e.getKey().getName(), e.getValue());
				}
				if (!hasType && !"".equals(type)) {
					v.setProperty("type", type);
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
			
			Edge e = graph.addEdge(a, vertices[(int) head], vertices[(int) tail], type);
			
			if (!supplyPropertiesAsIds) {
				for (Map.Entry<PropertyType, Object> p : properties.entrySet()) {
					e.setProperty(p.getKey().getName(), p.getValue());
				}
			}
		}

		
		public void propertyType(PropertyType type) {}
		public void vertexTypeStart(String type, long count) {}
		public void vertexTypeEnd(String type, long count) {}
		public void edgeTypeStart(String type, long count) {}
		public void edgeTypeEnd(String type, long count) {}
	}
}
