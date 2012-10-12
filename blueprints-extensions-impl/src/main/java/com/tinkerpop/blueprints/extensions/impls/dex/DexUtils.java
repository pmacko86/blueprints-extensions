package com.tinkerpop.blueprints.extensions.impls.dex;

import com.sparsity.dex.gdb.EdgesDirection;
import com.sparsity.dex.gdb.Graph;
import com.tinkerpop.blueprints.Direction;


/**
 * A collection of miscellaneous DEX utilities
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class DexUtils {

	
	/**
	 * Translate an edge direction from Blueprints to DEX
	 * 
	 * @param direction the Blueprints {@link Direction}
	 * @return the DEX {@link EdgesDirection}
	 */
	public static EdgesDirection translateDirection(Direction direction) {
		switch (direction) {
		case OUT : return EdgesDirection.Outgoing;
		case IN  : return EdgesDirection.Ingoing;
		case BOTH: return EdgesDirection.Any;
		default  : throw new IllegalArgumentException("Invalid direction"); 
		}
	}
	
	
	/**
	 * Get edge types
	 * 
	 * @param graph the DEX {@link Graph}
	 * @param label the edge label or null to get all edge types
	 * @return an array of edge types
	 */
	public static int[] getEdgeTypes(Graph graph, String label) {
		
		int[] types = null;
		
		if (label == null) {
			com.sparsity.dex.gdb.TypeList tlist = graph.findEdgeTypes();
			com.sparsity.dex.gdb.TypeListIterator typeItr = tlist.iterator();
			types = new int[tlist.count()];
			int ti = 0; while (typeItr.hasNext()) types[ti++] = typeItr.nextType();
			tlist.delete();
			tlist = null;
		}
		else {
			types = new int[1];
			types[0] = graph.findType(label);
		}
		
		return types;
	}
}
