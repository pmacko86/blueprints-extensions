package com.tinkerpop.blueprints.extensions.graphml;

import com.tinkerpop.blueprints.extensions.BulkloadableGraph;
import com.tinkerpop.blueprints.impls.neo4jbatch.Neo4jBatchGraph;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLTokens;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * GraphMLReader writes the data from a GraphML stream to a graph.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Alex Averbuch (alex.averbuch@gmail.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class FastGraphMLReader {
    private final Graph graph;

    private String vertexIdKey = null;
    private String edgeIdKey = null;
    private String edgeLabelKey = null;
    private FastGraphMLReaderProgressListener progressListener = null;
	private boolean ingestAsUndirected = false;

    /**
     * @param graph the graph to populate with the GraphML data
     */
    public FastGraphMLReader(Graph graph) {
        this.graph = graph;
    }

    /**
     * @param vertexIdKey if the id of a vertex is a &lt;data/&gt; property, fetch it from the data property.
     */
    public void setVertexIdKey(String vertexIdKey) {
        this.vertexIdKey = vertexIdKey;
    }

    /**
     * @param edgeIdKey if the id of an edge is a &lt;data/&gt; property, fetch it from the data property.
     */
    public void setEdgeIdKey(String edgeIdKey) {
        this.edgeIdKey = edgeIdKey;
    }

    /**
     * @param edgeLabelKey if the label of an edge is a &lt;data/&gt; property, fetch it from the data property.
     */
    public void setEdgeLabelKey(String edgeLabelKey) {
        this.edgeLabelKey = edgeLabelKey;
    }

    /**
     * @param progressListener the progress listener.
     */
    public void setProgressListener(FastGraphMLReaderProgressListener progressListener) {
        this.progressListener = progressListener;
    }

    /**
     * @param ingestAsUndirected if true, ingest a directed graph as an undirected graph by doubling-up all edges
     */
    public void setIngestAsUndirected(boolean ingestAsUndirected) {
        this.ingestAsUndirected = ingestAsUndirected;
    }

    /**
     * Input the GraphML stream data into the graph.
     * In practice, usually the provided graph is empty.
     *
     * @param graphMLInputStream an InputStream of GraphML data
     * @throws IOException thrown when the GraphML data is not correctly formatted
     */
    public void inputGraph(final InputStream graphMLInputStream) throws IOException {
        inputGraph(this.graph, graphMLInputStream, 1000, this.vertexIdKey, this.edgeIdKey,
        		this.edgeLabelKey, this.progressListener, this.ingestAsUndirected);
    }

    /**
     * Input the GraphML stream data into the graph.
     * In practice, usually the provided graph is empty.
     *
     * @param graphMLInputStream an InputStream of GraphML data
     * @param bufferSize         the amount of elements to hold in memory before committing a transactions (only valid for TransactionalGraphs)
     * @throws IOException thrown when the GraphML data is not correctly formatted
     */
    public void inputGraph(final InputStream graphMLInputStream, int bufferSize) throws IOException {
        inputGraph(this.graph, graphMLInputStream, bufferSize, this.vertexIdKey, this.edgeIdKey,
        		this.edgeLabelKey, this.progressListener, this.ingestAsUndirected);
    }

    /**
     * Input the GraphML stream data into the graph.
     * In practice, usually the provided graph is empty.
     *
     * @param graph              the graph to populate with the GraphML data
     * @param graphMLInputStream an InputStream of GraphML data
     * @throws IOException thrown when the GraphML data is not correctly formatted
     */
    public static void inputGraph(final Graph graph, final InputStream graphMLInputStream) throws IOException {
        inputGraph(graph, graphMLInputStream, 1000, null, null, null);
    }

    /**
     * Input the GraphML stream data into the graph.
     * In practice, usually the provided graph is empty.
     *
     * @param graph              the graph to populate with the GraphML data
     * @param graphMLInputStream an InputStream of GraphML data
     * @param progressListener   the progress listener
     * @throws IOException thrown when the GraphML data is not correctly formatted
     */
    public static void inputGraph(final Graph graph, final InputStream graphMLInputStream,
    		FastGraphMLReaderProgressListener progressListener) throws IOException {
        inputGraph(graph, graphMLInputStream, 1000, null, null, null, progressListener, false);
    }

    /**
     * Input the GraphML stream data into the graph.
     * More control over how data is streamed is provided by this method.
     *
     * @param graph              the graph to populate with the GraphML data
     * @param graphMLInputStream an InputStream of GraphML data
     * @param bufferSize         the amount of elements to hold in memory before committing a transactions (only valid for TransactionalGraphs)
     * @param vertexIdKey        if the id of a vertex is a &lt;data/&gt; property, fetch it from the data property.
     * @param edgeIdKey          if the id of an edge is a &lt;data/&gt; property, fetch it from the data property.
     * @param edgeLabelKey       if the label of an edge is a &lt;data/&gt; property, fetch it from the data property.
     * @throws IOException thrown when the GraphML data is not correctly formatted
     */
    public static void inputGraph(final Graph graph, final InputStream graphMLInputStream, int bufferSize, String vertexIdKey,
    		String edgeIdKey, String edgeLabelKey) throws IOException {
    	  inputGraph(graph, graphMLInputStream, bufferSize, vertexIdKey, edgeIdKey, edgeLabelKey, null, false);
    }

    /**
     * Input the GraphML stream data into the graph.
     * More control over how data is streamed is provided by this method.
     *
     * @param inputGraph         the graph to populate with the GraphML data
     * @param graphMLInputStream an InputStream of GraphML data
     * @param bufferSize         the amount of elements to hold in memory before committing a transactions (only valid for TransactionalGraphs)
     * @param vertexIdKey        if the id of a vertex is a &lt;data/&gt; property, fetch it from the data property.
     * @param edgeIdKey          if the id of an edge is a &lt;data/&gt; property, fetch it from the data property.
     * @param edgeLabelKey       if the label of an edge is a &lt;data/&gt; property, fetch it from the data property.
     * @param progressListener   the progress listener
     * @param ingestAsUndirected if true, ingest a directed graph as an undirected graph by doubling-up all edges
     * @throws IOException thrown when the GraphML data is not correctly formatted
     */
    public static void inputGraph(final Graph inputGraph, final InputStream graphMLInputStream, int bufferSize,
    		String vertexIdKey, String edgeIdKey, String edgeLabelKey, FastGraphMLReaderProgressListener progressListener,
    		boolean ingestAsUndirected) throws IOException {

    	XMLInputFactory inputFactory = XMLInputFactory.newInstance();

		if (inputGraph instanceof BulkloadableGraph) {
			((BulkloadableGraph) inputGraph).startBulkLoad();
		}

    	try {
    		
    		final Graph graph = inputGraph instanceof TransactionalGraph
    				? BatchGraph.wrap(inputGraph, bufferSize)
    				: inputGraph;
       		
    		final Features features = graph.getFeatures();
    		
    		boolean supplyPropertiesAsIds = inputGraph instanceof Neo4jBatchGraph;
    		boolean supplyIds = !features.ignoresSuppliedIds && !supplyPropertiesAsIds;  
    		
    		XMLStreamReader reader = inputFactory.createXMLStreamReader(graphMLInputStream);

    		Map<String, String> keyIdMap = new HashMap<String, String>();
    		Map<String, String> keyTypesMaps = new HashMap<String, String>();
    		// <Mapped ID String, ID Object>

    		Map<String, Vertex> vertexMap = new HashMap<String, Vertex>();
    		// Mapping between Source/Target IDs and "Property IDs"
    		// <Default ID String, Mapped ID String>
    		Map<String, String> vertexMappedIdMap = new HashMap<String, String>();

    		// Buffered Vertex Data
    		String vertexId = null;
    		Map<String, Object> vertexProps = new HashMap<String, Object>();
    		boolean inVertex = false;

    		// Buffered Edge Data
    		String edgeId = null;
    		String edgeLabel = null;
    		Vertex edgeInVertex = null;
    		Vertex edgeOutVertex = null;
    		Map<String, Object> edgeProps = new HashMap<String, Object>();
    		boolean inEdge = false;

    		// Counts
    		int numVertices = 0;
    		int numEdges = 0;

    		while (reader.hasNext()) {

    			Integer eventType = reader.next();
    			if (eventType.equals(XMLEvent.START_ELEMENT)) {
    				String elementName = reader.getName().getLocalPart();

    				if (elementName.equals(GraphMLTokens.KEY)) {
    					String id = reader.getAttributeValue(null, GraphMLTokens.ID);
    					String attributeName = reader.getAttributeValue(null, GraphMLTokens.ATTR_NAME);
    					String attributeType = reader.getAttributeValue(null, GraphMLTokens.ATTR_TYPE);
    					keyIdMap.put(id, attributeName);
    					keyTypesMaps.put(attributeName, attributeType);

    				} else if (elementName.equals(GraphMLTokens.NODE)) {
    					vertexId = reader.getAttributeValue(null, GraphMLTokens.ID);
    					if (vertexIdKey != null)
    						vertexMappedIdMap.put(vertexId, vertexId);
    					inVertex = true;
    					vertexProps.clear();

    				} else if (elementName.equals(GraphMLTokens.EDGE)) {
    					edgeId = reader.getAttributeValue(null, GraphMLTokens.ID);
    					edgeLabel = reader.getAttributeValue(null, GraphMLTokens.LABEL);
    					edgeLabel = edgeLabel == null ? GraphMLTokens._DEFAULT : edgeLabel;

    					String outVertexId = reader.getAttributeValue(null, GraphMLTokens.SOURCE);
    					String inVertexId = reader.getAttributeValue(null, GraphMLTokens.TARGET);

    					if (vertexIdKey == null) {
    						edgeOutVertex = vertexMap.get(outVertexId);
    						edgeInVertex = vertexMap.get(inVertexId);
    					} else {
    						edgeOutVertex = vertexMap.get(vertexMappedIdMap.get(outVertexId));
    						edgeInVertex = vertexMap.get(vertexMappedIdMap.get(inVertexId));
    					}


    					// Automatically create vertices if they do not already exist

    					if (null == edgeOutVertex) {
    						edgeOutVertex = graph.addVertex(outVertexId);
    						vertexMap.put(outVertexId, edgeOutVertex);
    						if (vertexIdKey != null)
    							// Default to standard ID system (in case no mapped ID is found later)
    						vertexMappedIdMap.put(outVertexId, outVertexId);
    					}
    					if (null == edgeInVertex) {
    						edgeInVertex = graph.addVertex(inVertexId);
    						vertexMap.put(inVertexId, edgeInVertex);
    						if (vertexIdKey != null)
    							// Default to standard ID system (in case no mapped ID is found later)
    							vertexMappedIdMap.put(inVertexId, inVertexId);
    					}

    					inEdge = true;
    					edgeProps.clear();

    				} else if (elementName.equals(GraphMLTokens.DATA)) {
    					String key = reader.getAttributeValue(null, GraphMLTokens.KEY);
    					String attributeName = keyIdMap.get(key);

    					if (attributeName != null) {
    						String value = reader.getElementText();

    						if (inVertex) {
    							if ((vertexIdKey != null) && (key.equals(vertexIdKey))) {
    								// Should occur at most once per Vertex
    								// Assumes single ID prop per Vertex
    								vertexMappedIdMap.put(vertexId, value);
    								vertexId = value;
    							} else
    								vertexProps.put(attributeName, typeCastValue(key, value, keyTypesMaps));
    						}
    						else if (inEdge) {
    							if ((edgeLabelKey != null) && (key.equals(edgeLabelKey)))
    								edgeLabel = value;
    							else if ((edgeIdKey != null) && (key.equals(edgeIdKey)))
    								edgeId = value;
    							else
    								edgeProps.put(attributeName, typeCastValue(key, value, keyTypesMaps));
    						}
    					}

    				}
    			} else if (eventType.equals(XMLEvent.END_ELEMENT)) {
    				String elementName = reader.getName().getLocalPart();

    				if (elementName.equals(GraphMLTokens.NODE)) {
    					Vertex currentVertex = vertexMap.get(vertexId);

    					if (currentVertex != null) {
    						throw new RuntimeException("Duplicate vertex with the same ID: " + vertexId);
    					}
    					else {
    						Object a = supplyIds ? vertexId : null;
    						Object _id = null;
    						if (supplyPropertiesAsIds) {
    							_id = vertexProps.remove("_id");
    							a = vertexProps;
    						}
    						currentVertex = graph.addVertex(a);
    						vertexMap.put(vertexId, currentVertex);
    						if (_id != null) currentVertex.setProperty("_id", _id);
    					}

    					if (!supplyPropertiesAsIds) {
	    					for (Entry<String, Object> prop : vertexProps.entrySet()) {
	    						currentVertex.setProperty(prop.getKey(), prop.getValue());
	    					}
    					}

    					vertexId = null;
    					vertexProps.clear();
    					inVertex = false;

    					numVertices++;
    					if (progressListener != null) {
    						if ((numVertices + numEdges) % 1000 == 0) {
    							progressListener.inputGraphProgress(numVertices, numEdges);
    						}
    					}

    				} else if (elementName.equals(GraphMLTokens.EDGE)) {
    					
    					Object a = supplyIds ? edgeId : null;
						Object _id = null;
						if (supplyPropertiesAsIds) {
							_id = vertexProps.remove("_id");
							a = vertexProps;
						}
						
    					Edge currentEdge = graph.addEdge(a, edgeOutVertex, edgeInVertex, edgeLabel);
    					if (_id != null) currentEdge.setProperty("_id", _id);

    					if (!supplyPropertiesAsIds) {
	    					for (Entry<String, Object> prop : edgeProps.entrySet()) {
	    						currentEdge.setProperty(prop.getKey(), prop.getValue());
	    					}
    					}

    					numEdges++;

    					if (ingestAsUndirected) {
    						// Don't check whether the edge we are about to add already exists (we might need to revisit this)

							Edge edge = graph.addEdge(supplyPropertiesAsIds ? a : null, edgeInVertex, edgeOutVertex, edgeLabel);

							if (!supplyPropertiesAsIds) {
								for (Entry<String, Object> prop : edgeProps.entrySet()) {
									edge.setProperty(prop.getKey(), prop.getValue());
								}
							}

							numEdges++;
    					}

    					edgeId = null;
    					edgeLabel = null;
    					edgeInVertex = null;
    					edgeOutVertex = null;
    					inEdge = false;
    					edgeProps.clear();

    					if (progressListener != null) {
    						if ((numVertices + numEdges) % 1000 == 0) {
    							progressListener.inputGraphProgress(numVertices, numEdges);
    						}
    					}
    				}
    			}
    		}

    		reader.close();

    		if (progressListener != null) {
    			progressListener.inputGraphProgress(numVertices, numEdges);
    		}
    	} catch (XMLStreamException xse) {
    		throw new IOException(xse);
    	}
    	finally {
    		if (inputGraph instanceof BulkloadableGraph) {
    			((BulkloadableGraph) inputGraph).stopBulkLoad();
    		}
    	}
    }

    private static Object typeCastValue(String key, String value, Map<String, String> keyTypes) {
        String type = keyTypes.get(key);
        if (null == type || type.equals(GraphMLTokens.STRING))
            return value;
        else if (type.equals(GraphMLTokens.FLOAT))
            return Float.valueOf(value);
        else if (type.equals(GraphMLTokens.INT))
            return Integer.valueOf(value);
        else if (type.equals(GraphMLTokens.DOUBLE))
            return Double.valueOf(value);
        else if (type.equals(GraphMLTokens.BOOLEAN))
            return Boolean.valueOf(value);
        else if (type.equals(GraphMLTokens.LONG))
            return Long.valueOf(value);
        else
            return value;
    }
}