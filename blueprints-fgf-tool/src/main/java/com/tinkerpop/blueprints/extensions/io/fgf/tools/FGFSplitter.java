package com.tinkerpop.blueprints.extensions.io.fgf.tools;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.EdgeType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.PropertyType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.VertexType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReaderHandler;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFWriter;
import com.tinkerpop.blueprints.extensions.io.fgf.tools.FGFTool.GraphReaderProgressListener;


/**
 * Fast Graph Format: File splitter
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class FGFSplitter {
	
	private static boolean verbose = false;
	
	
	/**
	 * Print the usage info for the tool
	 */
	private static void usage() {
		System.err.println(FGFTool.PROGRAM_LONG_NAME);
		System.err.println("");
		System.err.println("Usage: " + FGFTool.PROGRAM_NAME + " split [OPTIONS] FILE");
		System.err.println("");
		System.err.println("Options:");
		System.err.println("  --help               Print this help");
		System.err.println("  --output, -o FILE    Specify the output files");
		System.err.println("  --verbose, -v        Verbose (print progress)");
		System.err.println("  --weight, -w OBJ%    Specify the percentage of objects for the first file");
	}

    
    /**
     * Tool: Split a .fgf file into two files
     * 
     * @param tool the tool name
     * @param args the command-line arguments
     * @return the exit code
     * @throws IOException on I/O error
     */
    static int run(String tool, String[] args) throws Exception {
    	
    	double weight = 0.5;
    	
    	
    	// Parse the command-line options
    	
		OptionSet options;
		OptionParser parser = new OptionParser();
    	
		parser.accepts("help");
    	parser.accepts("o").withRequiredArg().ofType(String.class);
		parser.accepts("output").withRequiredArg().ofType(String.class);
    	parser.accepts("v");
		parser.accepts("verbose");
    	parser.accepts("w").withRequiredArg().ofType(String.class);
		parser.accepts("weight").withRequiredArg().ofType(String.class);
		
		try {
			options = parser.parse(args);
		}
		catch (Exception e) {
			System.err.println("Invalid options (please use --help for a list): " + e.getMessage());
			return 1;
		}
		
		
		// The command-line options
		
		verbose = options.has("v") || options.has("verbose");
		
		if (options.has("help") || options.nonOptionArguments().isEmpty()) {
			usage();
			return options.has("help") ? 0 : 1;
		}
		
		Vector<String> outputFiles = new Vector<String>();
		if (options.has("o") || options.has("output")) {
			for (Object a : options.has("o") ? options.valuesOf("o") : options.valuesOf("output")) {
				for (String s : a.toString().split("[,]")) {
					if (!"".equals(s)) outputFiles.add(s);
				}
			}
		}
		
		if (options.has("w") || options.has("weight")) {
			String s = options.valueOf(options.has("w") ? "w" : "weight").toString();
			if (s.endsWith("%")) s = s.substring(0, s.length() - 1);
			weight = Double.parseDouble(s) / 100.0;
			if (weight < 0 || weight > 1) {
				System.err.println("Invalid weight");
				return 1;
			}
		}
		
		
		// Check the output files
		
		if (outputFiles.isEmpty()) {
			System.err.println("Error: No output files were specified (please use the -o/--output option)");
    		return 1;
		}
		
		if (outputFiles.size() != 2) {
			System.err.println("Error: Exactly two output files need to be specified");
    		return 1;
		}
		
		for (String outputFile : outputFiles) {
	    	if (!outputFile.endsWith(".fgf")) {
	    		System.err.println("Error: Each output file needs to have the .fgf extension.");
	    		return 1;
	    	}
		}

		
		// Input file
		
		if (options.nonOptionArguments().size() > 1) {
			System.err.println("Too many arguments (please use --help)");
			return 1;
		}
		
		File inputFile = new File(options.nonOptionArguments().get(0));
		if (!inputFile.getName().endsWith(".fgf")) {
    		System.err.println("Error: The input file needs to have the .fgf extension.");
    		return 1;
    	}
		
		if (!inputFile.exists()) {
    		System.err.println("Error: The input file does not exist.");
    		return 1;
    	}
		
		
		// Split

		try {
			FGFReader reader = new FGFReader(inputFile);
			
			FGFReader.VertexType[] vertexTypes = reader.getVertexTypes();
			FGFReader.EdgeType[] edgeTypes = reader.getEdgeTypes();
			
			
			// Calculate how many vertices and edges to copy to the first file
			
			long vertices1 = 0;
			for (FGFReader.VertexType t : vertexTypes) {
				long count = (long) (t.size() * weight);
				t.setAux(new MyAux(count));
				vertices1 += count;
			}
			
			long edges1 = 0;
			for (FGFReader.EdgeType t : edgeTypes) {
				long count = (long) (t.size() * weight);
				t.setAux(new MyAux(count));
				edges1 += count;
			}
			
			
			// Create the writers
			
			FGFWriter writer1 = new FGFWriter(new File(outputFiles.get(0)),
					reader.getInitialVertexId(), reader.getInitialEdgeId());
			
			FGFWriter writer2 = new FGFWriter(new File(outputFiles.get(1)),
					reader.getInitialVertexId() + vertices1, reader.getInitialEdgeId() + edges1);
			
			
			// Split
			
			GraphReaderProgressListener l = verbose ? new GraphReaderProgressListener() : null;
			if (verbose) System.err.print("Processing    :");
			
			Splitter s = new Splitter(reader, writer1, writer2, l);
			reader.read(s);
			
			if (verbose) l.graphProgress((int) reader.getNumberOfVertices(), (int) reader.getNumberOfEdges());
			if (verbose) System.err.println();
			
			
			// Finalize
			
			if (verbose) System.err.print("Finalizing 1/2: ");
			writer1.close();
			if (verbose) System.err.println("done");
			
			if (verbose) System.err.print("Finalizing 2/2: ");
			writer2.close();
			if (verbose) System.err.println("done");
		}
		catch (Exception e) {
			if (e.getMessage().startsWith("Error: ")) {
				System.err.println(e.getMessage());
				return 1;
			}
			throw e;
		}
		
		return 0;
    }
    
    
    /**
     * The splitter
     */
    private static class Splitter implements FGFReaderHandler {
    	
    	private FGFReader reader;
    	private FGFWriter[] writers;
    	private GraphReaderProgressListener listener;
    	
    	private HashMap<String, Object> temp;    	
    	private long vertexId;
    	private long verticesLoaded;
    	private long edgesLoaded;
     	
    	
    	/**
    	 * Create an instance of Splitter
    	 * @param reader the reader
    	 * @param writer1 the first writer
    	 * @param writer2 the second writer
    	 */
    	public Splitter(FGFReader reader, FGFWriter writer1, FGFWriter writer2, GraphReaderProgressListener listener) {
    		
    		this.reader = reader;
    		this.writers = new FGFWriter[] { writer1, writer2 };
    		this.listener = listener;
    		
    		this.temp = new HashMap<String, Object>();
    		this.vertexId = this.reader.getInitialVertexId();
    		this.verticesLoaded = 0;
    		this.edgesLoaded = 0;
    	}


		/**
		 * Callback for a vertex
		 * 
		 * @param id the vertex ID
		 * @param type the vertex type
		 * @param properties the map of properties
		 */
		@Override
		public void vertex(long id, VertexType type,
				Map<PropertyType, Object> properties) {
			
			MyAux aux = (MyAux) type.getAux();
			temp.clear();
			for (Map.Entry<PropertyType, Object> p : properties.entrySet()) {
				temp.put(p.getKey().getName(), p.getValue());
			}
			
			FGFWriter w;
			if (aux.count > 0) {
				aux.count--;
				w = writers[0];
			}
			else {
				w = writers[1];
			}
			
			try {
				long l = w.writeVertex(type.getName(), temp);
				if (l != vertexId) {
					throw new IllegalStateException("Expected to generate vertex with ID " + vertexId + " but got back ID " + l);
				}
				vertexId++;
				verticesLoaded++;
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			if (listener != null && verticesLoaded % 100000 == 0) listener.graphProgress((int) verticesLoaded, (int) edgesLoaded);
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
		public void edge(long id, long head, long tail, EdgeType type,
				Map<PropertyType, Object> properties) {
			
			MyAux aux = (MyAux) type.getAux();
			temp.clear();
			for (Map.Entry<PropertyType, Object> p : properties.entrySet()) {
				temp.put(p.getKey().getName(), p.getValue());
			}
			
			FGFWriter w;
			if (aux.count > 0) {
				aux.count--;
				w = writers[0];
			}
			else {
				w = writers[1];
			}
			
			try {
				w.writeEdge(head, tail, type.getName(), temp);
				edgesLoaded++;
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			if (listener != null && edgesLoaded % 100000 == 0) listener.graphProgress((int) verticesLoaded, (int) edgesLoaded);
		}


		public void propertyType(PropertyType type) {}
		public void vertexTypeStart(VertexType type, long count) {}
		public void vertexTypeEnd(VertexType type, long count) {}
		public void edgeTypeStart(EdgeType type, long count) {}
		public void edgeTypeEnd(EdgeType type, long count) {}
    }
    
    
    /**
     * Auxiliary information for vertex and edge types
     */
    private static class MyAux {
    	
    	public long count = 0;
    	
    	public MyAux(long count) {
    		this.count = count;
    	}
    }
}
