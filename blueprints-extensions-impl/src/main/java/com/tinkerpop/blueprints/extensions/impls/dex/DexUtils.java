package com.tinkerpop.blueprints.extensions.impls.dex;

import com.sparsity.dex.gdb.EdgesDirection;
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
}
