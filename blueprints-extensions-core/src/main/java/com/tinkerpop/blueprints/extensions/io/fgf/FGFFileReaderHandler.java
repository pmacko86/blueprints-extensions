package com.tinkerpop.blueprints.extensions.io.fgf;

import java.util.Map;


/**
 * Fast Graph Format: Handler for the reader
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public interface FGFFileReaderHandler {


	/**
	 * Callback for a property type
	 * 
	 * @param type the property type object
	 */
	public void propertyType(FGFFileReader.PropertyType type);
	
	
	/**
	 * Callback for starting a new vertex type
	 * 
	 * @param type the vertex type
	 * @param count the number of vertices of the given type
	 */
	public void vertexTypeStart(FGFFileReader.VertexType type, long count);


	/**
	 * Callback for a vertex
	 * 
	 * @param id the vertex ID
	 * @param type the vertex type
	 * @param properties the map of properties
	 */
	public void vertex(long id, FGFFileReader.VertexType type, Map<FGFFileReader.PropertyType, Object> properties);
	
	
	/**
	 * Callback for starting the end of a vertex type
	 * 
	 * @param type the vertex type
	 * @param count the number of vertices of the given type
	 */
	public void vertexTypeEnd(FGFFileReader.VertexType type, long count);
	
	
	/**
	 * Callback for starting a new edge type
	 * 
	 * @param type the edge type
	 * @param count the number of edges of the given type
	 */
	public void edgeTypeStart(FGFFileReader.EdgeType type, long count);


	/**
	 * Callback for an edge
	 * 
	 * @param id the edge ID
	 * @param tail the tail vertex id (also known as the "out" or the "source" vertex)
	 * @param head the head vertex id (also known as the "in" or the "target" vertex)
	 * @param type the edge type (label)
	 * @param properties the map of properties
	 */
	public void edge(long id, long tail, long head, FGFFileReader.EdgeType type, Map<FGFFileReader.PropertyType, Object> properties);
	
	
	/**
	 * Callback for starting the end of an edge type
	 * 
	 * @param type the edge type
	 * @param count the number of edges of the given type
	 */
	public void edgeTypeEnd(FGFFileReader.EdgeType type, long count);
}
