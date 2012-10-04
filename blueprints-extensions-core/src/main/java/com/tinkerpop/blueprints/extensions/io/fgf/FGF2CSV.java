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
public class FGF2CSV {

	
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
			
			Handler handler = new Handler(reader, outputDir, prefix, listener);
			reader.read(handler);
		}
		finally {
			reader.close();
		}
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
		 */
		private void createFile(String type, boolean nodes) {
			
			try {
				if (out != null) out.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
					
			String encodedTypeName = "";
			for (int i = 0; i < type.length(); i++) {
				char c = type.charAt(i);
				if (Character.isLetterOrDigit(c) || c == '_') {
					encodedTypeName += c;
				}
				else {
					encodedTypeName += "-" + Integer.toHexString(((int) c) < 0 ? -((int) c) : (int) c);
				}
			}
			
			String fileName = prefix + "-" + (nodes ? "nodes" : "edges") + encodedTypeName + ".csv";
			file = new File(outputDir, fileName);
			
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
			
			createFile(type, nodes);
			
			
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
			try {
				if (out != null) out.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
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
			try {
				if (out != null) out.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}	
	}
}
