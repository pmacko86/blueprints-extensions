package com.tinkerpop.blueprints.extensions.fgf.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import com.tinkerpop.blueprints.extensions.fgf.FGFReader;
import com.tinkerpop.blueprints.extensions.fgf.FGFReaderHandler;
import com.tinkerpop.blueprints.extensions.fgf.FGFWriter;
import com.tinkerpop.blueprints.extensions.fgf.GraphML2FGF;
import com.tinkerpop.blueprints.extensions.fgf.FGFReader.PropertyType;
import com.tinkerpop.blueprints.extensions.graphml.FastGraphMLReaderProgressListener;


/**
 * Fast Graph Format: A suite of command-line tools
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class FGFTool {
	
	private static final String PROGRAM_NAME = "fgftool.sh";
	
	
	/**
	 * Print the usage info for the tool
	 */
	private static void usage() {
		System.err.println("Fast Graph Format -- a suite of command-line tools");
		System.err.println("");
		System.err.println("Usage: " + PROGRAM_NAME + " TOOL [OPTIONS]");
		System.err.println("");
		System.err.println("Tools:");
		System.err.println("  dump          Dump a .fgf file");
		System.err.println("  graphml2fgf   Convert a .graphml file to a .fgf file");
		System.err.println("  help          Print this help");
		System.err.println("  stat          Print graph statistics of a .fgf file");
	}
    
    
    /**
     * Stand-alone converter
     * 
     * @throws IOException on I/O error 
     */
    public static void main(String args[]) throws IOException {
    	
    	if (args.length == 0) {
    		usage();
    		return;
    	}
    	
    	String tool = args[0];
    	String[] toolArgs = new String[args.length - 1];
    	System.arraycopy(args, 1, toolArgs, 0, toolArgs.length);
    	
    	
	    try {
	    	
	    	// Tool: help
	    	
	    	if ("help".equals(tool)) {
	    		usage();
	    		return;
	    	}
	    	
	    	
	    	// Tool: dump
	    	
	    	if ("dump".equals(tool)) {
	    		System.exit(dump(tool, toolArgs));
	    	}
	    	
	    	
	    	// Tool: graphml2fgf
	    	
	    	if ("graphml2fgf".equals(tool)) {
	    		System.exit(graphml2fgf(tool, toolArgs));
	    	}
	    	
	    	
	    	// Tool: stat
	    	
	    	if ("stat".equals(tool)) {
	    		System.exit(stat(tool, toolArgs));
	    	}
	    }
	    catch (IOException e) {
    		System.err.println("Error: " + e.getMessage());
    		System.exit(1);	    	
	    }
     }

    
    /**
     * Tool: Dump a .fgf file
     * 
     * @param tool the tool name
     * @param args the command-line arguments
     * @return the exit code
     * @throws IOException on I/O error
     */
    private static int dump(String tool, String[] args) throws IOException {
    	
    	
    	// Parse the command-line options
    	
		OptionSet options;
		OptionParser parser = new OptionParser();
    	
		parser.accepts("help");
		
		try {
			options = parser.parse(args);
		}
		catch (Exception e) {
			System.err.println("Invalid options (please use --help for a list): " + e.getMessage());
			return 1;
		}
		
		List<String> l = options.nonOptionArguments();
		
		
		// Parse the command-line options: Options & help
		
		if (options.has("help") || l.size() != 1) {
			System.err.println("Fast Graph Format -- a suite of command-line tools");
			System.err.println("");
			System.err.println("Usage: " + PROGRAM_NAME + " " + tool + " [OPTIONS] INPUT.graphml");
			System.err.println("");
			System.err.println("Options:");
			System.err.println("  --help           Print this help");
			return options.has("help") ? 0 : 1;
		}
		
		
		// Parse the command-line options: Non-optional arguments
    	
    	String inputFile = l.get(0);
     	
    	
    	// Tool
    	
     	FGFReader r = new FGFReader(new File(inputFile));
    	try {
			r.read(new FGFReaderHandler() {
				
				private void printProperties(Map<PropertyType, Object> properties) {
					if (!properties.isEmpty()) {
						boolean first = true;
						for (Entry<PropertyType, Object> p : properties.entrySet()) {
							if (first) {
								System.out.print(" { ");
								first = false;
							}
							else {
								System.out.print(", ");
							}
							System.out.print(p.getKey().getName() + "=" + p.getValue());
						}
						System.out.print(" }");
					}
				}
				
				@Override
				public void vertexTypeStart(String type, long count) {
				}
				
				@Override
				public void vertexTypeEnd(String type, long count) {
				}
				
				@Override
				public void vertex(long id, String type,
						Map<PropertyType, Object> properties) {
					System.out.print("Node " + id + ", type " + type);
					printProperties(properties);
					System.out.println();
				}
				
				@Override
				public void propertyType(PropertyType type) {
				}
				
				@Override
				public void edgeTypeStart(String type, long count) {
				}
				
				@Override
				public void edgeTypeEnd(String type, long count) {
				}
				
				@Override
				public void edge(long id, long head, long tail, String type,
						Map<PropertyType, Object> properties) {
					System.out.print("Edge " + id + ": " + head + " ---> " + tail + ", type " + type);
					printProperties(properties);
					System.out.println();
				}
			});
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
    	r.close();

    	return 0;
    }

    
    /**
     * Tool: Convert a .graphml file to a .fgf file
     * 
     * @param tool the tool name
     * @param args the command-line arguments
     * @return the exit code
     * @throws IOException on I/O error
     */
    private static int graphml2fgf(String tool, String[] args) throws IOException {
    	
    	
    	// Parse the command-line options
    	
		OptionSet options;
		OptionParser parser = new OptionParser();
    	
		parser.accepts("help");
    	parser.accepts("v");
		parser.accepts("verbose");
		
		try {
			options = parser.parse(args);
		}
		catch (Exception e) {
			System.err.println("Invalid options (please use --help for a list): " + e.getMessage());
			return 1;
		}
		
		List<String> l = options.nonOptionArguments();
		
		
		// Parse the command-line options: Options & help
		
		if (options.has("help") || l.size() != 2) {
			System.err.println("Fast Graph Format -- a suite of command-line tools");
			System.err.println("");
			System.err.println("Usage: " + PROGRAM_NAME + " " + tool + " [OPTIONS] INPUT.graphml OUTPUT.fgf");
			System.err.println("");
			System.err.println("Options:");
			System.err.println("  --help           Print this help");
			System.err.println("  --verbose, -v    Verbose (print progress)");
			return options.has("help") ? 0 : 1;
		}
		
		boolean verbose = options.has("v") || options.has("verbose");
		
		
		// Parse the command-line options: Non-optional arguments
    	
    	String inputFile = l.get(0);
    	String outputFile = l.get(1);
    	
    	if (!outputFile.endsWith(".fgf")) {
    		System.err.println("Error: The output file needs to have the .fgf extension");
    		System.exit(1);
    	}
    	
    	
    	// Tool
    	
    	FileInputStream fin = new FileInputStream(inputFile);
    	FGFWriter out = new FGFWriter(new File(outputFile));
    	
    	if (verbose) System.err.print("Converting:");
    	GraphML2FGF.convert(fin, out, verbose ? new GraphMLReaderProgressListener() : null);
    	if (verbose) System.err.println();

    	return 0;
    }

    
    /**
     * Tool: Print graph statistics of a .fgf file
     * 
     * @param tool the tool name
     * @param args the command-line arguments
     * @return the exit code
     * @throws IOException on I/O error
     */
    private static int stat(String tool, String[] args) throws IOException {
    	
    	
    	// Parse the command-line options
    	
		OptionSet options;
		OptionParser parser = new OptionParser();
    	
		parser.accepts("help");
		
		try {
			options = parser.parse(args);
		}
		catch (Exception e) {
			System.err.println("Invalid options (please use --help for a list): " + e.getMessage());
			return 1;
		}
		
		List<String> l = options.nonOptionArguments();
		
		
		// Parse the command-line options: Options & help
		
		if (options.has("help") || l.size() != 1) {
			System.err.println("Fast Graph Format -- a suite of command-line tools");
			System.err.println("");
			System.err.println("Usage: " + PROGRAM_NAME + " " + tool + " [OPTIONS] INPUT.graphml");
			System.err.println("");
			System.err.println("Options:");
			System.err.println("  --help           Print this help");
			return options.has("help") ? 0 : 1;
		}
		
		
		// Parse the command-line options: Non-optional arguments
    	
    	String inputFile = l.get(0);
     	
    	
    	// Tool
    	
     	FGFReader r = new FGFReader(new File(inputFile));
    	try {
			r.read(new FGFReaderHandler() {
				
				@Override
				public void vertexTypeStart(String type, long count) {
					System.out.println("" + count + " node" + (count == 1 ? "" : "s") + " of type " + type);
				}
				
				@Override
				public void vertexTypeEnd(String type, long count) {
				}
				
				@Override
				public void vertex(long id, String type, Map<PropertyType, Object> properties) {
				}
				
				@Override
				public void propertyType(PropertyType type) {
					System.out.println("Property " + type.getName() + " of type #" + type.getType());
				}
				
				@Override
				public void edgeTypeStart(String type, long count) {
					System.out.println("" + count + " edge" + (count == 1 ? "" : "s") + " of type " + type);
				}
				
				@Override
				public void edgeTypeEnd(String type, long count) {
				}
				
				@Override
				public void edge(long id, long head, long tail, String type, Map<PropertyType, Object> properties) {
				}
			});
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
    	r.close();

    	return 0;
    }
   
    
    /**
     * Progress listener for the command-line tool
     */
    private static class GraphMLReaderProgressListener implements FastGraphMLReaderProgressListener {
    	
    	private static final String BACKSPACES = "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b";
    	
    	private String lastProgressString = "";
    	private long lastProgressTime = 0;
    	private long lastProgressObjectCount = 0;
    	
    	/**
    	 * Callback for when the given number of vertices and edges were loaded
    	 * 
    	 * @param graph the graph loaded so far
    	 * @param vertices the number of vertices loaded so far
    	 * @param edges the number of edges
    	 */
    	@Override
    	public void inputGraphProgress(int vertices, int edges) {
    		
    		long t = System.currentTimeMillis();
    		long dt = t - lastProgressTime;
    		
    		lastProgressTime = t;
    		long objects = vertices + edges;
    		long d = objects - lastProgressObjectCount; 
    		lastProgressObjectCount = objects;

    		String s = String.format(" %d vertices, %d edges (%.2f Kops/s)", vertices, edges, d/(double)dt);
    		System.err.print(getBackspaces(lastProgressString.length()));
    		System.err.print(s + "        " + getBackspaces(8));
    		lastProgressString = s;
    	}
    	
    	
    	/**
    	 * Get a string of backspaces of the given length
    	 * 
    	 * @param length the number of backspaces
    	 * @return the string of backspaces
    	 */
    	private String getBackspaces(int length) {
    		int l = BACKSPACES.length();
    		int n = length;
    		String s = "";
    		while (n >= l) {
    			s += BACKSPACES;
    			n -= l;
    		}
    		s += BACKSPACES.substring(l - n);
    		return s;
    	}
    }
}
