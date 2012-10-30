package com.tinkerpop.blueprints.extensions.impls.dex;

import com.sparsity.dex.gdb.Attribute;
import com.sparsity.dex.gdb.AttributeKind;
import com.sparsity.dex.gdb.DataType;
import com.sparsity.dex.gdb.EdgesDirection;
import com.sparsity.dex.gdb.Graph;
import com.sparsity.dex.gdb.Objects;
import com.sparsity.dex.gdb.Value;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFTypes;


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
	 * Translate a property data type from FGFTypes to DEX
	 * 
	 * @param type the FGF type code
	 * @return the DEX type
	 */
	public static DataType translateDataTypeFromFGF(short type) {
		
		switch (type) {
		
		case FGFTypes.OTHER  : throw new IllegalArgumentException("Unsupported FGF property type code");
		case FGFTypes.STRING : return DataType.String;

		case FGFTypes.BOOLEAN: return DataType.Boolean;
		case FGFTypes.SHORT  : return DataType.Integer;
		case FGFTypes.INTEGER: return DataType.Integer;
		case FGFTypes.LONG   : return DataType.Integer;

		case FGFTypes.FLOAT  : return DataType.Double;
		case FGFTypes.DOUBLE : return DataType.Double;
		
		default:
			throw new IllegalArgumentException("Invalid FGF property type code");
		}
	}
	
	
	/**
	 * Convert an Object to a Value
	 * 
	 * @param type the DEX data type
	 * @param value the Object value
	 * @param out the output Value (can be null)
	 * @return the DEX Value (the same as out, if specified)
	 */
	public static Value translateValue(DataType type, Object value, Value out) {
    	
    	// https://github.com/tinkerpop/blueprints/blob/master/...
    	//           blueprints-dex-graph/src/main/java/com/tinkerpop/blueprints/impls/...
    	//           dex/DexGraph.java
        
    	Value v = out == null ? new Value() : out;
    	
    	switch (type) {
        case Boolean:
            v.setBooleanVoid((Boolean) value);
            break;
        case Integer:
            v.setIntegerVoid((Integer) value);
            break;
        case Long:
            v.setLongVoid((Long) value);
            break;
        case String:
            v.setStringVoid((String) value);
            break;
        case Double:
            if (value instanceof Double) {
                v.setDoubleVoid((Double) value);
            } else if (value instanceof Float) {
                v.setDoubleVoid(((Float) value).doubleValue());
            }
            break;
        default:
            throw new UnsupportedOperationException();
    	}

    	return v;
	}
	
	
	/**
	 * Convert an Object to a Value
	 * 
	 * @param type the DEX data type
	 * @param value the Object value
	 * @return the DEX Value
	 */
	public static Value translateValue(DataType type, Object value) {
    	return translateValue(type, value, null);
	}
	
	
	/**
	 * Get all node types
	 * 
	 * @param graph the DEX {@link Graph}
	 * @return an array of node types
	 */
	public static int[] getNodeTypes(Graph graph) {
		
		int[] types = null;
		com.sparsity.dex.gdb.TypeList tlist = graph.findNodeTypes();
		com.sparsity.dex.gdb.TypeListIterator typeItr = tlist.iterator();
		types = new int[tlist.count()];
		int ti = 0; while (typeItr.hasNext()) types[ti++] = typeItr.nextType();
		tlist.delete();
		tlist = null;
		
		return types;
	}
	
	
	/**
	 * Get all node labels
	 * 
	 * @param graph the DEX {@link Graph}
	 * @return an array of node labels (type strings)
	 */
	public static String[] getNodeLabels(Graph graph) {
		
		String[] types = null;
		com.sparsity.dex.gdb.TypeList tlist = graph.findNodeTypes();
		com.sparsity.dex.gdb.TypeListIterator typeItr = tlist.iterator();
		types = new String[tlist.count()];
		int ti = 0; while (typeItr.hasNext()) types[ti++] = graph.getType(typeItr.nextType()).getName();
		tlist.delete();
		tlist = null;
		
		return types;
	}
	
	
	/**
	 * Get all edge types
	 * 
	 * @param graph the DEX {@link Graph}
	 * @return an array of edge types
	 */
	public static int[] getEdgeTypes(Graph graph) {
		
		int[] types = null;
		com.sparsity.dex.gdb.TypeList tlist = graph.findEdgeTypes();
		com.sparsity.dex.gdb.TypeListIterator typeItr = tlist.iterator();
		types = new int[tlist.count()];
		int ti = 0; while (typeItr.hasNext()) types[ti++] = typeItr.nextType();
		tlist.delete();
		tlist = null;
		
		return types;
	}
	
	
	/**
	 * Get all edge types
	 * 
	 * @param graph the DEX {@link Graph}
	 * @return an array of edge labels (type strings)
	 */
	public static String[] getEdgeLabels(Graph graph) {
		
		String[] types = null;
		com.sparsity.dex.gdb.TypeList tlist = graph.findEdgeTypes();
		com.sparsity.dex.gdb.TypeListIterator typeItr = tlist.iterator();
		types = new String[tlist.count()];
		int ti = 0; while (typeItr.hasNext()) types[ti++] = graph.getType(typeItr.nextType()).getName();
		tlist.delete();
		tlist = null;
		
		return types;
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
			types = getEdgeTypes(graph);
		}
		else {
			types = new int[1];
			types[0] = graph.findType(label);
		}
		
		return types;
	}
	
	
	/**
	 * Get objects using an index
	 * 
	 * @param graph the DEX {@link Graph}
	 * @param adata the attribute data
	 * @param value the attribute value
	 * @return an iterable object (must be closed by the caller)
	 */
	public static Objects getUsingIndex(final Graph graph, final Attribute adata, final Object value) {
    	
    	Value v = translateValue(adata.getDataType(), value);
        return graph.select(adata.getId(), com.sparsity.dex.gdb.Condition.Equal, v);
    }
	
	
	/**
	 * Get DEX attribute handle, creating it if necessary
	 * 
	 * @param graph the DEX graph
	 * @param objectType the object type
	 * @param key the key
	 * @param dataType the data type (to be used if creating)
	 * @param kind the attribute kind (to be used if creating)
	 */
	public static int getOrCreateAttributeHandle(Graph graph, int objectType, String key,
			DataType dataType, AttributeKind kind) {
		int attr = graph.findAttribute(objectType, key);
		if (attr == Attribute.InvalidAttribute) {
			attr = graph.newAttribute(objectType, key, dataType, kind);
		}
		return attr;
	}
	
	
	/**
	 * Find the attribute ID for each type in the array
	 * 
	 * @param graph the DEX graph
	 * @param types the types array
	 * @param key the attribute name
	 * @return the array of attribute IDs
	 */
	public static int[] findAttributes(Graph graph, int[] types, String key) {
		int[] r = new int[types.length];
		for (int i = 0; i < types.length; i++) {
			r[i] = graph.findAttribute(types[i], key);
		}
		return r;
	}
	
	
	/**
	 * Translate a Blueprints vertex to a DEX vertex
	 * 
	 * @param vertex the Blueprints vertex
	 * @return the DEX vertex
	 */
	public static long translateVertex(Vertex vertex) {
		return ((Long) vertex.getId()).longValue();
	}
	
	
	/**
	 * Translate an array of Blueprints vertices to DEX vertices
	 * 
	 * @param vertices the Blueprints vertices
	 * @return the DEX vertices
	 */
	public static long[] translateVertices(Vertex[] vertices) {
		long[] r = new long[vertices.length];
		for (int i = 0; i < vertices.length; i++) {
			r[i] = ((Long) vertices[i].getId()).longValue();
		}
		return r;
	}
	
	
	/**
	 * Translate a Blueprints edge to a DEX edge
	 * 
	 * @param vertex the Blueprints edge
	 * @return the DEX edge
	 */
	public static long translateEdge(Edge edge) {
		return ((Long) edge.getId()).longValue();
	}
	
	
	/**
	 * Translate an array of Blueprints edges to DEX edges
	 * 
	 * @param vertices the Blueprints edges
	 * @return the DEX edges
	 */
	public static long[] translateEdges(Edge[] edges) {
		long[] r = new long[edges.length];
		for (int i = 0; i < edges.length; i++) {
			r[i] = ((Long) edges[i].getId()).longValue();
		}
		return r;
	}
}
