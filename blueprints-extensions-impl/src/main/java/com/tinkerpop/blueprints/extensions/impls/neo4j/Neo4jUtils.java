package com.tinkerpop.blueprints.extensions.impls.neo4j;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jEdge;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jVertex;


/**
 * A collection of miscellaneous Neo4j utilities
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */

public class Neo4jUtils {

	
	/**
	 * Translate an edge direction from Blueprints to DEX
	 * 
	 * @param direction the Blueprints {@link Direction}
	 * @return the Neo4j {@link org.neo4j.graphdb.Direction}
	 */
	public static org.neo4j.graphdb.Direction translateDirection(Direction direction) {
		switch (direction) {
		case OUT : return org.neo4j.graphdb.Direction.OUTGOING;
		case IN  : return org.neo4j.graphdb.Direction.INCOMING;
		case BOTH: return org.neo4j.graphdb.Direction.BOTH;
		default  : throw new IllegalArgumentException("Invalid direction"); 
		}
	}
	
	
	/**
	 * Translate a Blueprints vertex to a Neo4j vertex
	 * 
	 * @param vertex the Blueprints vertex
	 * @return the Neo4j node
	 */
	public static Node translateVertex(Vertex vertex) {
		return ((Neo4jVertex) vertex).getRawVertex();
	}
	
	
	/**
	 * Translate an array of Blueprints vertices to Neo4j vertices
	 * 
	 * @param vertices the Blueprints vertices
	 * @return the Neo4j nodes
	 */
	public static Node[] translateVertices(Vertex[] vertices) {
		Node[] r = new Node[vertices.length];
		for (int i = 0; i < vertices.length; i++) {
			r[i] = ((Neo4jVertex) vertices[i]).getRawVertex();
		}
		return r;
	}
	
	
	/**
	 * Translate a Blueprints vertex to a Neo4j vertex
	 * 
	 * @param vertex the Blueprints vertex
	 * @return the Neo4j relationship
	 */
	public static Relationship translateEdge(Edge edge) {
		return ((Neo4jEdge) edge).getRawEdge();
	}
	
	
	/**
	 * Translate an array of Blueprints edges to Neo4j edges
	 * 
	 * @param vertices the Blueprints edges
	 * @return the Neo4j relationships
	 */
	public static Relationship[] translateEdges(Edge[] edges) {
		Relationship[] r = new Relationship[edges.length];
		for (int i = 0; i < edges.length; i++) {
			r[i] = ((Neo4jEdge) edges[i]).getRawEdge();
		}
		return r;
	}
}
