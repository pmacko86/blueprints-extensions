package com.tinkerpop.blueprints.extensions.impls.dex;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import com.sparsity.dex.gdb.AttributeKind;
import com.sparsity.dex.gdb.AttributeList;
import com.sparsity.dex.gdb.DataType;
import com.sparsity.dex.gdb.Int32List;
import com.sparsity.dex.gdb.StringList;
import com.sparsity.dex.io.CSVReader;
import com.sparsity.dex.io.EdgeTypeLoader;
import com.sparsity.dex.io.NodeTypeLoader;
import com.sparsity.dex.io.TypeLoaderEvent;
import com.sparsity.dex.io.TypeLoaderListener;
import com.tinkerpop.blueprints.extensions.io.GraphProgressListener;
import com.tinkerpop.blueprints.extensions.io.fgf.FGF2DexCSV;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFConstants;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFTypes;
import com.tinkerpop.blueprints.impls.dex.DexGraph;


/**
 * DEX Graph loader from CSV files generated by FGF2CSV
 * 
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class DexCSVLoader {
	
	
	/**
	 * Load from a directory of CSV files
	 * 
	 * @param graph the DEX graph
	 * @param dir the directory with the input files
	 * @param prefix the file name prefix
	 * @throws IOException on I/O or parse error
	 */
	public static void load(com.sparsity.dex.gdb.Graph graph, File dir, String prefix) throws IOException {
		load(graph, dir, prefix, null);
	}
	
	
	/**
	 * Load from a directory of CSV files and optionally index all properties
	 * 
	 * @param graph the DEX graph
	 * @param dir the directory with the input files
	 * @param prefix the file name prefix
	 * @param listener the progress listener
	 * @throws IOException on I/O or parse error
	 */
	public static void load(com.sparsity.dex.gdb.Graph graph, File dir, String prefix,
			GraphProgressListener listener) throws IOException {
		
		if (dir.exists() && !dir.isDirectory()) {
			throw new IOException("The specified directory is not a directory: " + dir.getName());
		}
		
		if (!dir.exists()) {
			throw new IOException("The specified directory does not exist: " + dir.getName());
		}
		
		
		// Get the lists of files
		
		ArrayList<File> nodeFiles = new ArrayList<File>();
		ArrayList<File> edgeFiles = new ArrayList<File>();
		
		String nodePrefix = prefix + "-nodes";
		String edgePrefix = prefix + "-edges";
		
		for (File f : dir.listFiles()) {
			if (!f.getName().endsWith(".csv")) continue;
			if (!f.getName().endsWith("-meta.csv")) {
				if (f.getName().startsWith(nodePrefix)) nodeFiles.add(f);
				if (f.getName().startsWith(edgePrefix)) edgeFiles.add(f);
			}
		}
		
		if (nodeFiles.isEmpty()) {
			throw new IOException("No nodes found");
		}
		if (edgeFiles.isEmpty()) {
			throw new IOException("No edges found");
		}
		
		if (nodeFiles.size() != 1) {
			throw new IOException("Only one DEX node type is currently supported");
		}
		
		
		// Wrap the listener
		
		GraphProgressListenerWrapper wrappedListener = null;
		if (listener != null) {
			wrappedListener = new GraphProgressListenerWrapper(listener);
		}
		
		
		// Load each node type
		
		int nodeIdAttribute = Integer.MIN_VALUE;
		
		for (File f : nodeFiles) {		
			
			// Get the type name
			
			assert f.getName().startsWith(nodePrefix) &&  f.getName().endsWith(".csv");
			String typeName = FGF2DexCSV.decodeFileNameFriendlyString(
					f.getName().substring(nodePrefix.length(), f.getName().length() - 4));
			
			
			// Read the .csv header and the metadata
			
			HashMap<String, Integer> csvAttributeToColumn = readAttributeToColumnMap(f);
			
			String metaFileName = nodePrefix + FGF2DexCSV.encodeToFileNameFriendlyString(typeName) + "-meta.csv";
			File metaFile = new File(f.getParentFile(), metaFileName);
			HashMap<String, DataType> attributeTypes = readPropertyTypes(metaFile);
			
			attributeTypes.put(FGFConstants.KEY_ORIGINAL_ID, DataType.Integer);
			csvAttributeToColumn.put(FGFConstants.KEY_ORIGINAL_ID, 0);


			// Create the new DEX type and its properties
			
			if ("".equals(typeName)) {
				typeName = DexGraph.DEFAULT_DEX_VERTEX_LABEL;
			}
			
			int type = graph.newNodeType(typeName);
			
			HashMap<String, Integer> attributeMap = new HashMap<String, Integer>();
			Int32List attributePositions = new Int32List();
			AttributeList attributes = new AttributeList();
			
			for (String s : attributeTypes.keySet()) {
				
				Integer column = csvAttributeToColumn.get(s);
				if (column == null) {
					throw new IOException("The attribute " + s + " does not appear in the .csv header");
				}
				
				AttributeKind ak = AttributeKind.Basic;
				if (s.equals(FGFConstants.KEY_ORIGINAL_ID)) ak = AttributeKind.Unique;
				int a = graph.newAttribute(type, s, attributeTypes.get(s), ak);
				
				attributeMap.put(s, a);
				attributePositions.add(column);
				attributes.add(a);
			}
			
			nodeIdAttribute = attributeMap.get(FGFConstants.KEY_ORIGINAL_ID);
			
			
			// Create the CSV reader
			
			CSVReader reader = new CSVReader();
			reader.setStartLine(1);
			reader.open(f.getAbsolutePath());
			
			
			// Create the node loader and fire it off
			
			NodeTypeLoader loader = new NodeTypeLoader(reader, graph, type, attributes, attributePositions);
			if (wrappedListener != null) {
				loader.setFrequency(10000);
				loader.register(wrappedListener);
			}
			loader.run();
			
			
			// Finish
			
			reader.close();
		}
		
		assert nodeIdAttribute != Integer.MIN_VALUE;
		
		
		// Load each edge type
		
		if (wrappedListener != null) wrappedListener.nodePhase = false;
		
		for (File f : edgeFiles) {		
			
			// Get the type name
			
			assert f.getName().startsWith(edgePrefix) &&  f.getName().endsWith(".csv");
			String typeName = FGF2DexCSV.decodeFileNameFriendlyString(
					f.getName().substring(edgePrefix.length(), f.getName().length() - 4));
			
			
			// Read the .csv header and the metadata
			
			HashMap<String, Integer> csvAttributeToColumn = readAttributeToColumnMap(f);
			
			String metaFileName = edgePrefix + FGF2DexCSV.encodeToFileNameFriendlyString(typeName) + "-meta.csv";
			File metaFile = new File(f.getParentFile(), metaFileName);
			HashMap<String, DataType> attributeTypes = readPropertyTypes(metaFile);

					
			// Create the new DEX type and its properties
			
			int type = graph.newEdgeType(typeName, true /* directed */, true /* materialized */);
			
			HashMap<String, Integer> attributeMap = new HashMap<String, Integer>();
			Int32List attributePositions = new Int32List();
			AttributeList attributes = new AttributeList();
			
			for (String s : attributeTypes.keySet()) {
				
				Integer column = csvAttributeToColumn.get(s);
				if (column == null) {
					throw new IOException("The attribute " + s + " does not appear in the .csv header");
				}
				
				boolean index = false;
				int a = graph.newAttribute(type, s, attributeTypes.get(s), index ? AttributeKind.Indexed : AttributeKind.Basic);
				attributeMap.put(s, a);
				attributePositions.add(column);
				attributes.add(a);
			}
			
			
			// Create the CSV reader
			
			CSVReader reader = new CSVReader();
			reader.setStartLine(1);
			reader.open(f.getAbsolutePath());
			
			
			// Create the node loader and fire it off
			
			EdgeTypeLoader loader = new EdgeTypeLoader(reader, graph, type,
					attributes, attributePositions, 0, 1, nodeIdAttribute, nodeIdAttribute);
			if (wrappedListener != null) {
				loader.setFrequency(10000);
				loader.register(wrappedListener);
			}
			loader.run();
			
			
			// Finish
			
			reader.close();
		}
	}
	
	
	/**
	 * Read the property name to the column index map from a header of a data .csv file
	 * 
	 * @param file the data file
	 * @return the map of property names to their column indices
	 * @throws IOException on an I/O error
	 */
	private static HashMap<String, Integer> readAttributeToColumnMap(File file) throws IOException {
		
		HashMap<String, Integer> csvAttributeToColumn = new HashMap<String, Integer>();
		
		CSVReader headerReader = new CSVReader();
		headerReader.setStartLine(0);
		headerReader.open(file.getAbsolutePath());
		
		StringList header = new StringList();
		if (!headerReader.read(header)) {
			try {
				headerReader.close();
			}
			catch (Exception e) {};
			throw new IOException("The .csv file is missing a header: " + file.getName());
		}
		
		int headerIndex = 0;
		for (String h : header) {
			csvAttributeToColumn.put(h, headerIndex++);
		}
		
		headerReader.close();
		
		return csvAttributeToColumn;
	}
	
	
	/**
	 * Read the map of property types from a metadata .csv file
	 * 
	 * @param metadataFile the file
	 * @return a map of property names to their DEX values
	 * @throws IOException on an I/O error
	 */
	private static HashMap<String, DataType> readPropertyTypes(File metadataFile) throws IOException { 
		
		HashMap<String, DataType> attributeTypes = new HashMap<String, DataType>();			
		
		CSVReader metaReader = new CSVReader();
		metaReader.setStartLine(1);
		metaReader.open(metadataFile.getAbsolutePath());
		
		StringList l = new StringList();
		while (metaReader.read(l)) {
			Iterator<String> i = l.iterator();
			
			String name = i.next();
			short type = FGFTypes.fromString(i.next());
			i.next(); // count
			
			attributeTypes.put(name, fgfType2DEXDataType(type));
		}
		
		metaReader.close();

		return attributeTypes;
	}

	
	/**
	 * Convert a FGF type code to a DEX DataType
	 * 
	 * @param type the FGF type code
	 * @return the equivalent DEX DataType code
	 */
	private static DataType fgfType2DEXDataType(short type) {

		switch (type) {
		
		case FGFTypes.OTHER  : throw new IllegalArgumentException("Unsupported FGF type: " + FGFTypes.toString(type));
		case FGFTypes.STRING : return DataType.String ;

		case FGFTypes.BOOLEAN: return DataType.Boolean;
		case FGFTypes.SHORT  : return DataType.Integer;
		case FGFTypes.INTEGER: return DataType.Integer;
		case FGFTypes.LONG   : return DataType.Integer;

		case FGFTypes.FLOAT  : return DataType.Double ;
		case FGFTypes.DOUBLE : return DataType.Double ;
		
		default:
			throw new IllegalArgumentException("Unsupported FGF type: " + FGFTypes.toString(type));
		}
	}
	
	
	/**
	 * A wrapper for GraphProgressListener
	 */
	private static class GraphProgressListenerWrapper extends TypeLoaderListener {
		
		private GraphProgressListener listener;
		
		public int nodes = 0;
		public int edges = 0;
		public boolean nodePhase = true;
		
		
		/**
		 * Create an instance of class GraphProgressListenerWrapper
		 * 
		 * @param listener the listener to wrap
		 */
		public GraphProgressListenerWrapper(GraphProgressListener listener) {
			this.listener = listener;
		}
		
		
		/**
		 * Callback
		 * 
		 * @param event the event
		 */
		@Override
		public void notifyEvent(TypeLoaderEvent event) {
			
			if (nodePhase) {
				listener.graphProgress(nodes + (int) event.getCount(), edges);
				if (event.isLast()) nodes += (int) event.getCount();
			}
			else {
				listener.graphProgress(nodes, edges + (int) event.getCount());
				if (event.isLast()) edges += (int) event.getCount();
			}
		}
	}
}
