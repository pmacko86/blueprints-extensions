package com.tinkerpop.blueprints.extensions.io.fgf;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.io.GraphProgressListener;


/**
 * Fast Graph Format: Blueprints Graph Exporter
 * 
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class FGFGraphExporter {
	
	
	/**
	 * Export an instance of a Graph to a FGF file
	 * 
	 * @param graph the input graph
	 * @param file the output file
	 * @throws IOException on I/O or parse error
	 */
	public static void export(Graph graph, File file) throws IOException {
		export(graph, file, false, null);
	}
	
	
	/**
	 * Export an instance of a Graph to a FGF file
	 * 
	 * @param graph the input graph
	 * @param file the output file
	 * @param keepOriginalId whether to keep the original ID property
	 * @param listener the progress listener
	 * @throws IOException on I/O or parse error
	 */
	public static void export(Graph graph, File file, boolean keepOriginalId, GraphProgressListener listener)
			throws IOException {
		
		// Open the writer
		
		FGFWriter writer = new FGFWriter(file);
		
		
		// Initialize
		
		int numVertices = 0;
		int numEdges = 0;
		
		Map<String, Object> properties = new TreeMap<String, Object>();
		Map<Object, Long> vertexMap = new HashMap<Object, Long>();
		
		
		// Export vertices
		
		Iterable<Vertex> vertices = graph.getVertices();
		for (Vertex v : vertices) {
			properties.clear();
			for (String k : v.getPropertyKeys()) {
				if (!keepOriginalId && k.equals(FGFGraphLoader.KEY_ORIGINAL_ID)) continue;
				properties.put(k, v.getProperty(k));
			}
			vertexMap.put(v.getId(), writer.writeVertex("", properties));
			
			if (listener != null && (numVertices % 10000) == 0) {
				listener.graphProgress(numVertices, numEdges);
			}
			
			numVertices++;
		}
		if (vertices instanceof CloseableIterable<?>) {
			((CloseableIterable<Vertex>) vertices).close();
		}
		
		
		// Export edges
		
		Iterable<Edge> edges = graph.getEdges();
		for (Edge e : edges) {
			properties.clear();
			for (String k : e.getPropertyKeys()) {
				properties.put(k, e.getProperty(k));
			}
			
			writer.writeEdge(vertexMap.get(e.getVertex(Direction.IN).getId()).longValue(),
					vertexMap.get(e.getVertex(Direction.OUT).getId()).longValue(),
					e.getLabel(), properties);
			
			if (listener != null && (numEdges % 10000) == 0) {
				listener.graphProgress(numVertices, numEdges);
			}
			
			numEdges++;
		}
		if (edges instanceof CloseableIterable<?>) {
			((CloseableIterable<Edge>) edges).close();
		}
    		
    		
    	// Finish
		
		writer.close();
    		
		if (listener != null) {
			listener.graphProgress(numVertices, numEdges);
		}
	}
}
