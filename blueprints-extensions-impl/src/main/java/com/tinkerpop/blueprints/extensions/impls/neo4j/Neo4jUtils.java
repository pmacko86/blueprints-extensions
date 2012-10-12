package com.tinkerpop.blueprints.extensions.impls.neo4j;

import com.tinkerpop.blueprints.Direction;


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
}
