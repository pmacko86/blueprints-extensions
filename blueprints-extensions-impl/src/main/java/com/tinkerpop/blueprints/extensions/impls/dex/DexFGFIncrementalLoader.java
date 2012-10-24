package com.tinkerpop.blueprints.extensions.impls.dex;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.sparsity.dex.gdb.Attribute;
import com.sparsity.dex.gdb.AttributeKind;
import com.sparsity.dex.gdb.DataType;
import com.sparsity.dex.gdb.Graph;
import com.sparsity.dex.gdb.Objects;
import com.sparsity.dex.gdb.ObjectsIterator;
import com.sparsity.dex.gdb.Type;
import com.sparsity.dex.gdb.Value;
import com.tinkerpop.blueprints.extensions.io.GraphProgressListener;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFGraphLoader;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.EdgeType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.PropertyType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.VertexType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReaderHandler;
import com.tinkerpop.blueprints.impls.dex.DexGraph;


/**
 * Fast Graph Format: DEX incremental Graph loader
 * 
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class DexFGFIncrementalLoader {
	
	
	/**
	 * Load from a FGF file
	 * 
	 * @param graph the batch graph
	 * @param file the input file
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public static void load(DexGraph graph, File file) throws IOException, ClassNotFoundException {
		load(graph, file, false, null);
	}	
	
	
	/**
	 * Load from a FGF file and optionally index all properties
	 * 
	 * @param graph the batch graph
	 * @param file the input file
	 * @param indexAllProperties whether to index all properties
	 * @param listener the progress listener
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public static void load(DexGraph graph, File file,
			boolean indexAllProperties, GraphProgressListener listener)
			throws IOException, ClassNotFoundException {
		
		FGFReader reader = new FGFReader(file);

		Loader l = new Loader(graph, reader, indexAllProperties, listener);
		reader.read(l);
		l.finish();
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
		
		private final DexGraph blueprintsGraph;
		private final Graph graph;
		private FGFReader reader;
		private boolean indexAllProperties;
		private GraphProgressListener listener;
		
		private int type;
		private Value temp;		
		private int attrId;
		private int[] attrIds;
		
		private long vertexIdUpperBound;
		private long[] vertices;
		private long verticesLoaded;
		private long edgesLoaded;
		private int vertexTypeIndex;
		
		@SuppressWarnings("unused")
		private int opsSinceCommit;
		
		
		/**
		 * Create an instance of class Loader
		 * 
		 * @param graph the graph
		 * @param reader the input file reader
		 * @param indexAllProperties whether to index all properties
		 * @param listener the progress listener
		 */
		public Loader(DexGraph graph, FGFReader reader, boolean indexAllProperties, GraphProgressListener listener) {
			
			this.blueprintsGraph = graph;
			this.graph = this.blueprintsGraph.getRawGraph();
			this.reader = reader;
			this.indexAllProperties = indexAllProperties;
			this.listener = listener;
			
			this.attrIds = new int[reader.getVertexTypes().length];
			
			this.vertexIdUpperBound = this.reader.getInitialVertexId() + this.reader.getNumberOfVertices(); 
			this.vertices = new long[(int) this.vertexIdUpperBound];
			this.verticesLoaded = 0;
			this.edgesLoaded = 0;
			this.vertexTypeIndex = 0;
			this.opsSinceCommit = 0;
			
			if (vertices[0] != Objects.InvalidOID) {
				for (int i = 0; i < vertices.length; i++) {
					vertices[i] = Objects.InvalidOID;
				}
			}
			
			this.attrId = Attribute.InvalidAttribute;
			
			this.type = Type.InvalidType;
			this.temp = new Value();
		}
		
		
		/**
		 * Finish the loading process
		 */
		public void finish() {
			opsSinceCommit = 0;
		}
		

		/**
		 * Callback for a property type
		 * 
		 * @param type the property type object
		 */
		@Override
		public void propertyType(PropertyType type) {
			type.setAux(new PropertyTypeAux());
		}
		
		
		/**
		 * Get DEX attribute handle, creating it if necessary
		 * 
		 * @param type the property type
		 * @return the handle 
		 */
		private int getOrCreateAttributeHandle(PropertyType type) {
			assert !type.getName().equals(FGFGraphLoader.KEY_ORIGINAL_ID);
			int attr = ((PropertyTypeAux) type.getAux()).attr;
			if (attr != Attribute.InvalidAttribute) return attr;
			attr = DexUtils.getOrCreateAttributeHandle(graph, this.type, type.getName(),
					DexUtils.translateDataTypeFromFGF(type.getType()),
					indexAllProperties ? AttributeKind.Indexed : AttributeKind.Basic);
			((PropertyTypeAux) type.getAux()).attr = attr;
			((PropertyTypeAux) type.getAux()).type = graph.getAttribute(attr).getDataType();
			return attr;
		}

		
		/**
		 * Callback for starting a new vertex type
		 * 
		 * @param type the vertex type
		 * @param count the number of vertices of the given type
		 */
		@Override
		public void vertexTypeStart(VertexType type, long count) {
			
			// The vertex type
			
			boolean newType = false;
			String typeName = type.getName();
			if ("".equals(typeName)) {
				typeName = DexGraph.DEFAULT_DEX_VERTEX_LABEL;
			}
			
			this.type = graph.findType(typeName);
			if (this.type == Type.InvalidType) {
				this.type = graph.newNodeType(typeName);
				newType = true;
			}
			
			
			// The vertex properties (attributes)
			
			for (PropertyType t : reader.getPropertyTypes()) {
				((PropertyTypeAux) t.getAux()).attr = Attribute.InvalidAttribute;
			}
			
			
			// The original ID attribute
			
			if (!newType) {
				this.attrId = graph.findAttribute(this.type, FGFGraphLoader.KEY_ORIGINAL_ID);
				if (this.attrId == Attribute.InvalidAttribute) {
					throw new RuntimeException("Attribute " + FGFGraphLoader.KEY_ORIGINAL_ID + " does not exist");
				}
			}
			else {
				this.attrId = DexUtils.getOrCreateAttributeHandle(graph, this.type,
						FGFGraphLoader.KEY_ORIGINAL_ID, DataType.Integer, AttributeKind.Unique);
			}
			this.attrIds[vertexTypeIndex] = this.attrId;
			
			
			// Index check
			
			Attribute attrIdData = graph.getAttribute(attrId);
			if (attrIdData.getKind() == AttributeKind.Basic) {
				throw new RuntimeException(FGFGraphLoader.KEY_ORIGINAL_ID + " is not indexed");
			}
		}
		
		
		/**
		 * Find a vertex by ID
		 * 
		 * @param id the ID
		 * @return the vertex OID
		 */
		private long findVertexById(long id) {
			
			temp.setInteger((int) id);
			long oid = Objects.InvalidOID;
			
			// Note that this does not check for the case if we have multiple vertex types,
			// and the key lookup matches for multiple vertex types. But this should not be
			// an issue, unless we try to ingest FGF files with overlapping vertex IDs (which
			// would be bad anyways).
			
			for (int attr : attrIds) {
				Objects objs = graph.select(attr, com.sparsity.dex.gdb.Condition.Equal, temp);
				ObjectsIterator objsItr = objs.iterator();
				
				if (objsItr.hasNext()) {
					oid = objsItr.nextObject();
					if (objsItr.hasNext()) {
						objsItr.close();
						objs.close();
						throw new RuntimeException("There is more than one vertex with " + FGFGraphLoader.KEY_ORIGINAL_ID + " " + id);
					}
				}

				objsItr.close();
				objs.close();
				
				if (oid != Objects.InvalidOID) return oid;
			}
			
			throw new RuntimeException("Cannot find vertex with " + FGFGraphLoader.KEY_ORIGINAL_ID + " " + id);
		}

		
		/**
		 * Callback for a vertex
		 * 
		 * @param id the vertex ID
		 * @param type the vertex type
		 * @param properties the map of properties
		 */
		@Override
		public void vertex(long id, VertexType type, Map<PropertyType, Object> properties) {
			
			long n = graph.newNode(this.type);
			vertices[(int) id] = n;
			verticesLoaded++;
			opsSinceCommit++;			
			
			
			// Properties
			
			for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
				int attr = getOrCreateAttributeHandle(e.getKey());
				Value v = DexUtils.translateValue(((PropertyTypeAux) e.getKey().getAux()).type, e.getValue(), temp);
				graph.setAttribute(n, attr, v);
				opsSinceCommit++;
			}
			
			temp.setInteger((int) id);
			graph.setAttribute(n, attrId, temp);
			opsSinceCommit++;
			
			
			// Callback
			
			if (listener != null && verticesLoaded % 10000 == 0) {
				listener.graphProgress((int) verticesLoaded, 0);
			}
		}

		
		/**
		 * Callback for starting the end of a vertex type
		 * 
		 * @param type the vertex type
		 * @param count the number of vertices of the given type
		 */
		@Override
		public void vertexTypeEnd(VertexType type, long count) {			
			vertexTypeIndex++;
		}
		
		
		/**
		 * Callback for starting a new edge type
		 * 
		 * @param type the edge type
		 * @param count the number of edges of the given type
		 */
		@Override
		public void edgeTypeStart(EdgeType type, long count) {
			
			this.type = graph.findType(type.getName());
			if (this.type == com.sparsity.dex.gdb.Type.InvalidType) {
				this.type = graph.newEdgeType(type.getName(), true, true);
			}
			
			for (PropertyType t : reader.getPropertyTypes()) {
				((PropertyTypeAux) t.getAux()).attr = Attribute.InvalidAttribute;
			}
			
			this.attrId = Attribute.InvalidAttribute;
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
		public void edge(long id, long head, long tail, EdgeType type, Map<PropertyType, Object> properties) {
			
			// Look up the head and tail vertices; attempt to use the key indexes if the corresponding
			// Node objects are not readily available
		
			long t = vertices[(int) tail];
			long h = vertices[(int) head];
			
			if (t == Objects.InvalidOID) {
				t = findVertexById(tail);
				vertices[(int) tail] = t;
			}
			if (h == Objects.InvalidOID) {
				h = findVertexById(head);
				vertices[(int) head] = h;
			}
			
			
			// Create the relationship
			
			long r = graph.newEdge(this.type, t, h);
			edgesLoaded++;
			opsSinceCommit++;
			
			
			// Add properties

			for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
				int attr = getOrCreateAttributeHandle(e.getKey());
				Value v = DexUtils.translateValue(((PropertyTypeAux) e.getKey().getAux()).type, e.getValue(), temp);
				graph.setAttribute(r, attr, v);
				opsSinceCommit++;
			}
			
			
			// Listener callback
			
			if (listener != null && edgesLoaded % 10000 == 0) {
				listener.graphProgress((int) verticesLoaded, (int) edgesLoaded);
			}
		}

		
		/**
		 * Callback for starting the end of an edge type
		 * 
		 * @param type the edge type
		 * @param count the number of edges of the given type
		 */
		@Override
		public void edgeTypeEnd(EdgeType type, long count) {
		}
		
		
		/**
		 * Additional property information
		 */
		private static class PropertyTypeAux {
			
			public int attr = Attribute.InvalidAttribute;
			public DataType type = null;
			
			
			/**
			 * Create an instance of type PropertyTypeAux
			 */
			public PropertyTypeAux() {
				// Nothing to do
			}
		}
	}
}
