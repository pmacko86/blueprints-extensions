package com.tinkerpop.blueprints.extensions.impls.neo4j;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.IndexHits;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.io.GraphProgressListener;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFConstants;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFFileReader.EdgeType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFFileReader.PropertyType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFFileReader;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFFileReader.VertexType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFFileReaderHandler;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.util.StringFactory;


/**
 * Fast Graph Format: Neo4j incremental Graph loader
 * 
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class Neo4jFGFIncrementalLoader {
	
	
	/**
	 * Load from a FGF file
	 * 
	 * @param graph the batch graph
	 * @param file the input file
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public static void load(Neo4jGraph graph, File file) throws IOException, ClassNotFoundException {
		load(graph, file, 100000, null);
	}	
	
	
	/**
	 * Load from a FGF file and optionally index all properties
	 * 
	 * @param graph the batch graph
	 * @param file the input file
	 * @param txBuffer the number of operations before a commit
	 * @param listener the progress listener
	 * @throws IOException on I/O or parse error
	 * @throws ClassNotFoundException on property unmarshalling error due to a missing class
	 */
	public static void load(Neo4jGraph graph, File file,
			int txBuffer, GraphProgressListener listener)
			throws IOException, ClassNotFoundException {
		
		FGFFileReader reader = new FGFFileReader(file);

		Loader l = new Loader(graph, reader, txBuffer, false, listener);
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
	private static class Loader implements FGFFileReaderHandler {
		
		private final Neo4jGraph blueprintsGraph;
		private final GraphDatabaseService graph;
		private final AutoIndexer<Node> nodeIndexer;
		private FGFFileReader reader;
		private boolean indexAllProperties;
		private GraphProgressListener listener;
		
		private long vertexIdUpperBound;
		private Node[] vertices;
		private DynamicRelationshipType relationshipType;
		private long verticesLoaded;
		private long edgesLoaded;
		
		private int txBuffer;
		private int opsSinceCommit;
		private Transaction tx;
		
		private boolean hasAdditionalVertexLabel;
		private boolean additionalVertexLabelIndexCreated;
		private boolean originalVertexIdIndexCreated;
		private Set<String> preexistingVertexPropertyIndexes;
		private Set<String> preexistingEdgePropertyIndexes;
		
		
		/**
		 * Create an instance of class Loader
		 * 
		 * @param graph the graph
		 * @param reader the input file reader
		 * @param txBuffer the number of operations before a commit
		 * @param indexAllProperties whether to index all properties
		 * @param listener the progress listener
		 */
		public Loader(Neo4jGraph graph, FGFFileReader reader, int txBuffer, boolean indexAllProperties, GraphProgressListener listener) {
			
			this.blueprintsGraph = graph;
			this.graph = this.blueprintsGraph.getRawGraph();
			this.nodeIndexer = this.graph.index().getNodeAutoIndexer();
			this.reader = reader;
			this.txBuffer = txBuffer;
			this.indexAllProperties = indexAllProperties;
			this.listener = listener;
			
			this.vertexIdUpperBound = this.reader.getInitialVertexId() + this.reader.getNumberOfVertices(); 
			this.vertices = new Node[(int) this.vertexIdUpperBound];
			this.relationshipType = null;
			this.verticesLoaded = 0;
			this.edgesLoaded = 0;
			this.opsSinceCommit = 0;
			
			this.hasAdditionalVertexLabel = false;
			this.additionalVertexLabelIndexCreated = false;
			this.originalVertexIdIndexCreated = false;
			
			preexistingVertexPropertyIndexes = blueprintsGraph.getIndexedKeys(Vertex.class);
			preexistingEdgePropertyIndexes   = blueprintsGraph.getIndexedKeys(Edge.class);
			
		
			// Index check
			
			if (!nodeIndexer.isEnabled() || !nodeIndexer.getAutoIndexedProperties().contains(FGFConstants.KEY_ORIGINAL_ID)) {
				throw new RuntimeException(FGFConstants.KEY_ORIGINAL_ID + " is not indexed");
			}
			
			
			// Start a transaction
			
			blueprintsGraph.commit();
			
			tx = this.graph.beginTx();
		}
		
		
		/**
		 * Finish the loading process
		 */
		public void finish() {
			tx.success();
			tx.finish();
			tx = null;
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
		 * Callback for starting a new vertex type
		 * 
		 * @param type the vertex type
		 * @param count the number of vertices of the given type
		 */
		@Override
		public void vertexTypeStart(VertexType type, long count) {
			// Nothing to do
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
			
			Node n = graph.createNode();
			vertices[(int) id] = n;
			verticesLoaded++;
			opsSinceCommit++;
			
			/*if (n.getId() == 0) {
				System.err.println("\nWarning: Created node with ID " + n.getId()
						+ " for " + FGFConstants.KEY_ORIGINAL_ID + "=" + id);
			}*/
			
			
			// Properties
			
			boolean hasLabel = false;
			for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
				n.setProperty(e.getKey().getName(), e.getValue());
				((PropertyTypeAux) e.getKey().getAux()).vertexKeyUsed = true;
				if (e.getKey().getName().equals(StringFactory.LABEL)) hasLabel = true;
				opsSinceCommit++;
			}
			if (!hasLabel && !"".equals(type.getName())) {
				n.setProperty(StringFactory.LABEL, type.getName());
				hasAdditionalVertexLabel = true;
				opsSinceCommit++;
			}
			n.setProperty(FGFConstants.KEY_ORIGINAL_ID, (int) id);
			opsSinceCommit++;
			
			
			// Commit periodically
			
			if (opsSinceCommit >= txBuffer) {
				tx.success();
				tx.finish();
				tx = this.graph.beginTx();
				opsSinceCommit = 0;
			}
			
			
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
			
			if (!originalVertexIdIndexCreated) {
				if (!preexistingVertexPropertyIndexes.contains(FGFConstants.KEY_ORIGINAL_ID)) {
					if (tx != null) {
						tx.success();
						tx.finish();
						tx = null;
					}
					blueprintsGraph.createKeyIndex(FGFConstants.KEY_ORIGINAL_ID, Vertex.class);
				}
				originalVertexIdIndexCreated = true;
			}
			
			if (indexAllProperties && hasAdditionalVertexLabel && !additionalVertexLabelIndexCreated) {
				if (!preexistingVertexPropertyIndexes.contains(StringFactory.LABEL)) {
					if (tx != null) {
						tx.success();
						tx.finish();
						tx = null;
					}
					blueprintsGraph.createKeyIndex(StringFactory.LABEL, Vertex.class);
				}
				additionalVertexLabelIndexCreated = true;
			}
			
			if (indexAllProperties) {
				for (PropertyType t : reader.getPropertyTypes()) {
					PropertyTypeAux x = (PropertyTypeAux) t.getAux();
					if (x.vertexKeyUsed && !x.vertexIndexCreated) {
						if (!preexistingVertexPropertyIndexes.contains(t.getName())) {
							if (tx != null) {
								tx.success();
								tx.finish();
								tx = null;
							}
							blueprintsGraph.createKeyIndex(t.getName(), Vertex.class);
						}
						x.vertexIndexCreated = true;
					}
				}
			}
			
			if (tx == null) {
				blueprintsGraph.commit();
				tx = this.graph.beginTx();
			}
		}
		
		
		/**
		 * Callback for starting a new edge type
		 * 
		 * @param type the edge type
		 * @param count the number of edges of the given type
		 */
		@Override
		public void edgeTypeStart(EdgeType type, long count) {
			relationshipType = DynamicRelationshipType.withName(type.getName());
		}

		
		/**
		 * Callback for an edge
		 * 
		 * @param id the edge ID
		 * @param tail the tail vertex id (also known as the "out" or the "source" vertex)
		 * @param head the head vertex id (also known as the "in" or the "target" vertex)
		 * @param type the edge type (label)
		 * @param properties the map of properties
		 */
		@Override
		public void edge(long id, long tail, long head, EdgeType type, Map<PropertyType, Object> properties) {
			
			// Look up the head and tail vertices; attempt to use the key indexes if the corresponding
			// Node objects are not readily available
		
			Node t = vertices[(int) tail];
			Node h = vertices[(int) head];
			
			if (t == null || h == null) {
				if (t == null) {
					Iterable<Node> i = nodeIndexer.getAutoIndex().get(FGFConstants.KEY_ORIGINAL_ID, (int) tail);
					Iterator<Node> itr = i.iterator();
					if (itr.hasNext()) t = itr.next();
					boolean b = itr.hasNext();
					if (i instanceof IndexHits) ((IndexHits<?>) i).close();
					if (t == null) throw new RuntimeException("Cannot find vertex with " + FGFConstants.KEY_ORIGINAL_ID + " " + tail);
					if (b) throw new RuntimeException("There is more than one vertex with " + FGFConstants.KEY_ORIGINAL_ID + " " + tail);
					vertices[(int) tail] = t;
				}
				
				if (h == null) {
					Iterable<Node> i = nodeIndexer.getAutoIndex().get(FGFConstants.KEY_ORIGINAL_ID, (int) head);
					Iterator<Node> itr = i.iterator();
					if (itr.hasNext()) h = itr.next();
					boolean b = itr.hasNext();
					if (i instanceof IndexHits) ((IndexHits<?>) i).close();
					if (h == null) throw new RuntimeException("Cannot find vertex with " + FGFConstants.KEY_ORIGINAL_ID + " " + head);
					if (b) throw new RuntimeException("There is more than one vertex with " + FGFConstants.KEY_ORIGINAL_ID + " " + head);
					vertices[(int) head] = h;
				}
			}
			
			
			// Create the relationship
			
			Relationship r;
			try {
				r = t.createRelationshipTo(h, relationshipType);
				edgesLoaded++;
				opsSinceCommit++;
			}
			catch (org.neo4j.graphdb.NotFoundException e) {
				System.err.println("\nError while creating a relationship: " + e.getMessage());
				System.err.println("  Tail: node[" + t.getId() + "], " + FGFConstants.KEY_ORIGINAL_ID + "=" + tail);
				System.err.println("  Head: node[" + h.getId() + "], " + FGFConstants.KEY_ORIGINAL_ID + "=" + head);
				System.err.println("  Type: " + relationshipType);
				throw e;
			}
			
			
			// Add properties

			for (Map.Entry<PropertyType, Object> e : properties.entrySet()) {
				r.setProperty(e.getKey().getName(), e.getValue());
				((PropertyTypeAux) e.getKey().getAux()).edgeKeyUsed = true;
				opsSinceCommit++;
			}
			
			
			// Commit periodically
			
			if (opsSinceCommit >= txBuffer) {
				tx.success();
				tx.finish();
				tx = this.graph.beginTx();
				opsSinceCommit = 0;
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
			
			if (indexAllProperties) {
				for (PropertyType t : reader.getPropertyTypes()) {
					PropertyTypeAux x = (PropertyTypeAux) t.getAux();
					if (x.edgeKeyUsed && !x.edgeIndexCreated) {
						if (!preexistingEdgePropertyIndexes.contains(t.getName())) {
							if (tx != null) {
								tx.finish();
								tx = null;
							}
							blueprintsGraph.createKeyIndex(t.getName(), Edge.class);
						}
						x.edgeIndexCreated = true;
					}
				}
			}
			
			if (tx == null) {
				blueprintsGraph.commit();
				tx = this.graph.beginTx();
			}
		}
		
		
		/**
		 * Additional property information
		 */
		private static class PropertyTypeAux {
			
			public boolean vertexKeyUsed = false;
			public boolean vertexIndexCreated = false;
			public boolean edgeKeyUsed = false;
			public boolean edgeIndexCreated = false;
			
			/**
			 * Create an instance of type PropertyTypeAux
			 */
			public PropertyTypeAux() {
				// Nothing to do
			}
		}
	}
}
