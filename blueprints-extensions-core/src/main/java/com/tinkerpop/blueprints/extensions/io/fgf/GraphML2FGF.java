package com.tinkerpop.blueprints.extensions.io.fgf;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import com.tinkerpop.blueprints.extensions.io.GraphProgressListener;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLTokens;


/**
 * Fast Graph Format: GraphML converter
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class GraphML2FGF {
	
	
    /**
     * Convert the GraphML stream data into the FGF format.
     *
     * @param graphMLInputStream an InputStream of GraphML data
     * @param fgfWriter          the FGF writer
     * @throws IOException thrown when the GraphML data is not correctly formatted
     */
    public static void convert(final InputStream graphMLInputStream, final FGFWriter fgfWriter) throws IOException {
    	convert(graphMLInputStream, fgfWriter, null, null, null, null);
    }
	
	
    /**
     * Convert the GraphML stream data into the FGF format.
     *
     * @param graphMLInputStream an InputStream of GraphML data
     * @param fgfWriter          the FGF writer
     * @param progressListener   the progress listener
     * @throws IOException thrown when the GraphML data is not correctly formatted
     */
    public static void convert(final InputStream graphMLInputStream, final FGFWriter fgfWriter,
    		GraphProgressListener progressListener) throws IOException {
    	convert(graphMLInputStream, fgfWriter, progressListener, null, null, null);
    }
    
	
    /**
     * Convert the GraphML stream data into the FGF format.
     *
     * @param graphMLInputStream an InputStream of GraphML data
     * @param fgfWriter          the FGF writer
     * @param progressListener   the progress listener
     * @param vertexIdKey        if the id of a vertex is a &lt;data/&gt; property, fetch it from the data property.
     * @param edgeIdKey          if the id of an edge is a &lt;data/&gt; property, fetch it from the data property.
     * @param edgeLabelKey       if the label of an edge is a &lt;data/&gt; property, fetch it from the data property.
     * @throws IOException thrown when the GraphML data is not correctly formatted
     */
    public static void convert(final InputStream graphMLInputStream, final FGFWriter fgfWriter,
    		GraphProgressListener progressListener,
    		String vertexIdKey, String edgeIdKey, String edgeLabelKey) throws IOException {

    	XMLInputFactory inputFactory = XMLInputFactory.newInstance();

    	try {
    		
    		XMLStreamReader reader = inputFactory.createXMLStreamReader(graphMLInputStream);

    		Map<String, String> keyIdMap = new HashMap<String, String>();
    		Map<String, String> keyTypesMaps = new HashMap<String, String>();
    		// <Mapped ID String, ID Object>

    		Map<String, Long> vertexMap = new HashMap<String, Long>();
    		// Mapping between Source/Target IDs and "Property IDs"
    		// <Default ID String, Mapped ID String>
    		Map<String, String> vertexMappedIdMap = new HashMap<String, String>();

    		// Buffered Vertex Data
    		String vertexId = null;
    		long vertexLongId = 0;
    		Map<String, Object> vertexProps = new HashMap<String, Object>();
    		boolean inVertex = false;

    		// Buffered Edge Data
    		@SuppressWarnings("unused")
			String edgeId = null;
    		String edgeLabel = null;
    		Long edgeInVertex = null;
    		Long edgeOutVertex = null;
    		Map<String, Object> edgeProps = new HashMap<String, Object>();
    		boolean inEdge = false;
    		
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
    					if (vertexIdKey != null) vertexMappedIdMap.put(vertexId, vertexId);
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
    						edgeOutVertex = fgfWriter.writeVertex("", null);
    						vertexMap.put(outVertexId, edgeOutVertex);
    						if (vertexIdKey != null)
    							// Default to standard ID system (in case no mapped ID is found later)
    							vertexMappedIdMap.put(outVertexId, outVertexId);
    					}
    					if (null == edgeInVertex) {
    						edgeInVertex = fgfWriter.writeVertex("", null);
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
    					
    					vertexLongId = fgfWriter.writeVertex("", vertexProps);
       					vertexMap.put(vertexId, vertexLongId);
    					numVertices++;
 
    					vertexId = null;
    					vertexProps.clear();
    					inVertex = false;
    					
    					if (progressListener != null) {
    						if ((numVertices + numEdges) % 1000 == 0) {
    							progressListener.graphProgress(numVertices, numEdges);
    						}
    					}
    					
    					if ((numVertices + numEdges) % 100000 == 0) System.gc();

    				} else if (elementName.equals(GraphMLTokens.EDGE)) {
    					
    					fgfWriter.writeEdge(edgeInVertex /* head/target */, edgeOutVertex /* tail/source */, edgeLabel, edgeProps);
    					numEdges++;

    					edgeId = null;
    					edgeLabel = null;
    					edgeInVertex = null;
    					edgeOutVertex = null;
    					inEdge = false;
    					edgeProps.clear();
    					
    					if (progressListener != null) {
    						if ((numVertices + numEdges) % 1000 == 0) {
    							progressListener.graphProgress(numVertices, numEdges);
    						}
    					}
    				}
    			}
    		}

    		reader.close();
    		fgfWriter.close();
    		
    		if (progressListener != null) {
    			progressListener.graphProgress(numVertices, numEdges);
    		}
    		
    	} catch (XMLStreamException xse) {
    		throw new IOException(xse);
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
