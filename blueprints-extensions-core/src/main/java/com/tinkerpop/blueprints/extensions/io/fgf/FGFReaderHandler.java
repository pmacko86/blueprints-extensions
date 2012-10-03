package com.tinkerpop.blueprints.extensions.io.fgf;

import java.util.Map;


/**
 * Fast Graph Format: Handler for the reader
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public interface FGFReaderHandler {


	/**
	 * Callback for a property type
	 * 
	 * @param type the property type object
	 */
	public void propertyType(FGFReader.PropertyType type);
	
	
	/**
	 * Callback for starting a new vertex type
	 * 
	 * @param type the vertex type
	 * @param count the number of vertices of the given type
	 */
	public void vertexTypeStart(String type, long count);


	/**
	 * Callback for a vertex
	 * 
	 * @param id the vertex ID
	 * @param type the vertex type
	 * @param properties the map of properties
	 */
	public void vertex(long id, String type, Map<FGFReader.PropertyType, Object> properties);
	
	
	/**
	 * Callback for starting the end of a vertex type
	 * 
	 * @param type the vertex type
	 * @param count the number of vertices of the given type
	 */
	public void vertexTypeEnd(String type, long count);
	
	
	/**
	 * Callback for starting a new edge type
	 * 
	 * @param type the edge type
	 * @param count the number of edges of the given type
	 */
	public void edgeTypeStart(String type, long count);


	/**
	 * Callback for an edge
	 * 
	 * @param id the edge ID
	 * @param head the vertex at the head
	 * @param tail the vertex at the tail
	 * @param type the edge type (label)
	 * @param properties the map of properties
	 */
	public void edge(long id, long head, long tail, String type, Map<FGFReader.PropertyType, Object> properties);
	
	
	/**
	 * Callback for starting the end of an edge type
	 * 
	 * @param type the edge type
	 * @param count the number of edges of the given type
	 */
	public void edgeTypeEnd(String type, long count);
}
