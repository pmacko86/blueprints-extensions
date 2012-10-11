package com.tinkerpop.blueprints.extensions.io.fgf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;

import com.tinkerpop.blueprints.extensions.io.GraphProgressListener;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.PropertyType;


/**
 * Fast Graph Format: CVS exporter
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class FGF2DexCSV {

	
	/**
	 * Convert a FGF into a series of multiple CSV files
	 * 
	 * @param inputFile the input FGF file
	 * @param outputDir the output directory for the CSV files
	 * @param prefix the file name prefix for the CSV files
	 * @param listener the graph progress listener
	 * @throws IOException on I/O error
	 * @throws ClassNotFoundException on property value unmarshalling error
	 */
	public static void convert(File inputFile, File outputDir, String prefix,
			GraphProgressListener listener) throws IOException, ClassNotFoundException {

		FGFReader reader = new FGFReader(inputFile);
		
		try {
			outputDir.mkdirs();
			
			if (listener != null) {
				listener.graphProgress(0, 0);
			}
			
			Handler handler = new Handler(reader, outputDir, prefix, listener);
			reader.read(handler);
			
			if (listener != null) {
				listener.graphProgress((int) reader.getNumberOfVertices(), (int) reader.getNumberOfEdges());
			}
		}
		finally {
			reader.close();
		}
	}
	
	
	/**
	 * Encode a string to a file name friendly string
	 * 
	 * @param str the string
	 * @return the encoded string
	 */
	public static String encodeToFileNameFriendlyString(String str) {
		
		StringBuilder r = new StringBuilder();
		
		for (int i = 0; i < str.length(); i++) {
			char c = str.charAt(i);
			
			if (Character.isLetterOrDigit(c) || c == '_') {
				r.append(c);
			}
			else {
				r.append('-');
				r.append(Integer.toHexString(((int) c) < 0 ? -((int) c) : (int) c));
			}
		}
		
		return r.toString();
	}
	
	
	/**
	 * Decode a file name friendly string
	 * 
	 * @param str the encoded string
	 * @return the decoded string
	 */
	public static String decodeFileNameFriendlyString(String str) {
		
		StringBuilder r = new StringBuilder();
		
		for (int i = 0; i < str.length(); i++) {
			char c = str.charAt(i);
			
			if (Character.isLetterOrDigit(c) || c == '_') {
				r.append(c);
			}
			else if (c == '-') {
				r.append((char) Integer.parseInt(str.substring(i + 1, i + 3), 16));
				i += 2;
			}
			else {
				throw new IllegalArgumentException("Unexpected character in a string: " + c);
			}
		}
		
		return r.toString();
	}
	
	
	/**
	 * The FGF reader handler
	 */
	private static class Handler implements FGFReaderHandler {
		
		private FGFReader reader;
		private File outputDir;
		private String prefix;
		private GraphProgressListener listener;
		
		private File file;
		private BufferedWriter out;
		private long nodesProcessed;
		private long edgesProcessed;
		private long listenerCycle;
		
		
		/**
		 * Create an instance of class Handler
		 * 
		 * @param reader the FGF reader
		 * @param outputDir the output directory for the CSV files
		 * @param prefix the file name prefix for the CSV files
		 * @param listener the graph progress listener
		 */
		public Handler(FGFReader reader, File outputDir, String prefix, GraphProgressListener listener) {
			
			this.reader = reader;
			this.outputDir = outputDir;
			this.prefix = prefix;
			this.listener = listener;
			
			this.file = null;
			this.out = null;
			this.nodesProcessed = 0;
			this.edgesProcessed = 0;
			this.listenerCycle = 0;
		}
		
		
		/**
		 * Create a file object
		 * 
		 * @param type the type string
		 * @param nodes true if these are nodes
		 * @param meta true for the metadata file
		 */
		private void createFile(String type, boolean nodes, boolean meta) {
			
			// Close the previous file, if it is still open
			
			try {
				if (out != null) out.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			
			// Compose the new file name
			
			String fileName = prefix + "-" + (nodes ? "nodes" : "edges")
					+ encodeToFileNameFriendlyString(type) + (meta ? "-meta" : "") + ".csv";
			
			file = new File(outputDir, fileName);
			
			
			// Open the new file
			
			try {
				out = new BufferedWriter(new FileWriter(file));
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		
		/**
		 * Create a CSV file with a header and initialize the internal house-keeping
		 * 
		 * @param type the type string
		 * @param nodes true if these are nodes
		 */
		private void startNewType(String type, boolean nodes) {
			
			// Create the file
			
			createFile(type, nodes, false);
			
			
			// Write the header
			
			try {
				if (nodes) {
					out.write("_id");
				}
				else {
					out.write("_head,_tail");
				}
				for (PropertyType p : reader.getPropertyTypes()) {
					out.write(',');
					StringEscapeUtils.escapeCsv(out, p.getName());
				}
				out.newLine();
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			
			// Prepare the custom property metadata
			
			for (PropertyType p : reader.getPropertyTypes()) {
				p.setAux(new MyPropertyMeta());
			}
		}
		
		
		/**
		 * Finish the node or the edge type
		 * 
		 * @param type the type string
		 * @param nodes true if these are nodes
		 */
		private void finishType(String type, boolean nodes) {
			
			// Close
			
			try {
				if (out != null) out.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			
			// Create the metadata file
			
			createFile(type, nodes, true);
			
			
			// Write the header
			
			try {
				out.write("property,type,count");
				out.newLine();
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			
			// Write the column metadata, but only for the property types that
			// were used at least once
			
			try {
				for (PropertyType p : reader.getPropertyTypes()) {
					if (((MyPropertyMeta) p.getAux()).count > 0) {
						StringEscapeUtils.escapeCsv(out, p.getName());
						out.write(',');
						out.write(FGFTypes.toString(p.getType()));
						out.write(',');
						out.write("" + ((MyPropertyMeta) p.getAux()).count);
						out.newLine();
					}
				}
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			
			// Finish
			
			try {
				if (out != null) out.close();
				out = null;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		

		/**
		 * Callback for a property type
		 * 
		 * @param type the property type object
		 */
		@Override
		public void propertyType(PropertyType type) {
			//
		}

		
		/**
		 * Callback for starting a new vertex type
		 * 
		 * @param type the vertex type
		 * @param count the number of vertices of the given type
		 */
		@Override
		public void vertexTypeStart(String type, long count) {
			startNewType(type, true);
			listenerCycle = nodesProcessed % 100000;
		}


		/**
		 * Callback for a vertex
		 * 
		 * @param id the vertex ID
		 * @param type the vertex type
		 * @param properties the map of properties
		 */
		@Override
		public void vertex(long id, String type,
				Map<PropertyType, Object> properties) {
			
			try {
				out.write(String.valueOf(id));
				for (PropertyType p : reader.getPropertyTypes()) {
					out.write(',');
					Object value = properties.get(p);
					if (value != null) {
						StringEscapeUtils.escapeCsv(out, value.toString());
						((MyPropertyMeta) p.getAux()).count++;
					}
				}
				out.newLine();
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			nodesProcessed++;
			if ((++listenerCycle) >= 100000 && listener != null) {
				listenerCycle = 0;
				listener.graphProgress((int) nodesProcessed, (int) edgesProcessed);
			}
		}

		
		/**
		 * Callback for starting the end of a vertex type
		 * 
		 * @param type the vertex type
		 * @param count the number of vertices of the given type
		 */
		@Override
		public void vertexTypeEnd(String type, long count) {
			finishType(type, true);
		}

		
		/**
		 * Callback for starting a new edge type
		 * 
		 * @param type the edge type
		 * @param count the number of edges of the given type
		 */
		@Override
		public void edgeTypeStart(String type, long count) {
			startNewType(type, false);
			listenerCycle = edgesProcessed % 100000;
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
		public void edge(long id, long head, long tail, String type,
				Map<PropertyType, Object> properties) {

			try {
				out.write(String.valueOf(head));
				out.write(',');
				out.write(String.valueOf(tail));
				for (PropertyType p : reader.getPropertyTypes()) {
					out.write(',');
					Object value = properties.get(p);
					if (value != null) {
						StringEscapeUtils.escapeCsv(out, value.toString());
						((MyPropertyMeta) p.getAux()).count++;
					}
				}
				out.newLine();
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			edgesProcessed++;
			if ((++listenerCycle) >= 100000 && listener != null) {
				listenerCycle = 0;
				listener.graphProgress((int) nodesProcessed, (int) edgesProcessed);
			}
		}

		
		/**
		 * Callback for starting the end of an edge type
		 * 
		 * @param type the edge type
		 * @param count the number of edges of the given type
		 */
		@Override
		public void edgeTypeEnd(String type, long count) {
			finishType(type, false);
		}
		
		
		/**
		 * Additional property type metadata
		 */
		private class MyPropertyMeta {
			
			/// The count
			public int count;
			
			
			/**
			 * Create an instance of class MyPropertyMeta
			 */
			public MyPropertyMeta() {
				count = 0;
			}
		}
	}
}
