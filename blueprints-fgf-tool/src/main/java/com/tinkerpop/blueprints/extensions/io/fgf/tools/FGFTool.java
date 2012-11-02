package com.tinkerpop.blueprints.extensions.io.fgf.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import com.tinkerpop.blueprints.extensions.io.GraphProgressListener;
import com.tinkerpop.blueprints.extensions.io.fgf.FGF2DexCSV;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.EdgeType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.VertexType;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReaderHandler;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFTypes;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFWriter;
import com.tinkerpop.blueprints.extensions.io.fgf.GraphML2FGF;
import com.tinkerpop.blueprints.extensions.io.fgf.FGFReader.PropertyType;


/**
 * Fast Graph Format: A suite of command-line tools
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class FGFTool {
	
	static final String PROGRAM_NAME = "fgftool.sh";
	static final String PROGRAM_LONG_NAME = "Fast Graph Format -- a suite of command-line tools";
	
	
	/**
	 * Print the usage info for the tool
	 */
	private static void usage() {
		System.err.println(PROGRAM_LONG_NAME);
		System.err.println("");
		System.err.println("Usage: " + PROGRAM_NAME + " TOOL [OPTIONS]");
		System.err.println("");
		System.err.println("Tools:");
		System.err.println("  dump          Dump a .fgf file");
		System.err.println("  fgf2dexcsv    Convert a .fgf file to a set of DEX .csv files");
		System.err.println("  generate      Generate a graph and save it as .fgf");
		System.err.println("  graphml2fgf   Convert a .graphml file to a .fgf file");
		System.err.println("  help          Print this help");
		System.err.println("  pairs2fgf     Convert a file with node pairs to a .fgf file");
		System.err.println("  split         Split a .fgf file into two files");
		System.err.println("  stat          Print graph statistics of a .fgf file");
	}
    
    
    /**
     * Stand-alone converter
     * 
     * @throws IOException on I/O error 
     */
    public static void main(String args[]) throws Exception {
    	
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
	    	
	    	
	    	// Tool: fgf2dexcsv
	    	
	    	if ("fgf2dexcsv".equals(tool)) {
	    		System.exit(fgf2dexcsv(tool, toolArgs));
	    	}
	    	
	    	
	    	// Tool: generate
	    	
	    	if ("generate".equals(tool)) {
	    		System.exit(FGFGraphGenerator.run(tool, toolArgs));
	    	}
	    	
	    	
	    	// Tool: graphml2fgf
	    	
	    	if ("graphml2fgf".equals(tool)) {
	    		System.exit(graphml2fgf(tool, toolArgs));
	    	}
	    	
	    	
	    	// Tool: pairs2fgf
	    	
	    	if ("pairs2fgf".equals(tool)) {
	    		System.exit(Pairs2FGF.run(tool, toolArgs));
	    	}
	    	
	    	
	    	// Tool: split
	    	
	    	if ("split".equals(tool)) {
	    		System.exit(FGFSplitter.run(tool, toolArgs));
	    	}
	    	
	    	
	    	// Tool: stat
	    	
	    	if ("stat".equals(tool)) {
	    		System.exit(stat(tool, toolArgs));
	    	}
	    	
	    	
	    	// Error
	    	
    		System.err.println("Error: Invalid tool (please use \"" + PROGRAM_NAME + " help\" for a list)");
    		System.exit(1);
	    }
	    catch (IOException e) {
    		System.err.println("Error: " + e.getMessage());
    		System.exit(1);
	    }
	    catch (Throwable e) {
	    	System.err.println("Error: " + e.getMessage());
	    	e.printStackTrace(System.err);
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
    	
		parser.accepts("edges-only");
		parser.accepts("help");
		parser.accepts("help");
		parser.accepts("nodes-only");
		parser.accepts("vertices-only");
		
		try {
			options = parser.parse(args);
		}
		catch (Exception e) {
			System.err.println("Error: Invalid options (please use --help for a list): " + e.getMessage());
			return 1;
		}
		
		List<String> l = options.nonOptionArguments();
		
		
		// Parse the command-line options: Options & help
		
		if (options.has("help") || l.size() != 1) {
			System.err.println(PROGRAM_LONG_NAME);
			System.err.println("");
			System.err.println("Usage: " + PROGRAM_NAME + " " + tool + " [OPTIONS] INPUT.fgf");
			System.err.println("");
			System.err.println("Options:");
			System.err.println("  --edges-only     Print only edges");
			System.err.println("  --help           Print this help");
			System.err.println("  --nodes-only     Print only vertices (an alias for --vertices-only)");
			System.err.println("  --vertices-only  Print only vertices");
			return options.has("help") ? 0 : 1;
		}
		
		boolean verticesOnly = options.has("nodes-only") || options.has("vertices-only");
		boolean edgesOnly = options.has("edges-only");
		if (verticesOnly && edgesOnly) {
			System.err.println("Error: Cannot combine --edges-only with " 
					+ (options.has("nodes-only") ? "--nodes-only" :  "--vertices-only"));
			return 1;
		}
		
		final boolean printVertices = (!verticesOnly && !edgesOnly) || verticesOnly;
		final boolean printEdges    = (!verticesOnly && !edgesOnly) ||    edgesOnly;
		
		
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
				public void vertexTypeStart(VertexType type, long count) {
				}
				
				@Override
				public void vertexTypeEnd(VertexType type, long count) {
				}
				
				@Override
				public void vertex(long id, VertexType type,
						Map<PropertyType, Object> properties) {
					if (printVertices) {
						System.out.print("Node " + id + ", type " + ("".equals(type.getName()) ? "<default>" : type.getName()));
						printProperties(properties);
						System.out.println();
					}
				}
				
				@Override
				public void propertyType(PropertyType type) {
				}
				
				@Override
				public void edgeTypeStart(EdgeType type, long count) {
				}
				
				@Override
				public void edgeTypeEnd(EdgeType type, long count) {
				}
				
				@Override
				public void edge(long id, long head, long tail, EdgeType type,
						Map<PropertyType, Object> properties) {
					if (printEdges) {
						System.out.print("Edge " + id + ": " + tail + " ---> " + head
								+ ", type " + ("".equals(type.getName()) ? "<default>" : type.getName()));
						printProperties(properties);
						System.out.println();
					}
				}
			});
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
    	r.close();

    	return 0;
    }

    
    /**
     * Tool: Convert a .fgf file to a set of DEX .csv files
     * 
     * @param tool the tool name
     * @param args the command-line arguments
     * @return the exit code
     * @throws IOException on I/O error
     * @throws ClassNotFoundException on property unmarshalling error
     */
    private static int fgf2dexcsv(String tool, String[] args) throws IOException, ClassNotFoundException {
    	
    	
    	// Parse the command-line options
    	
		OptionSet options;
		OptionParser parser = new OptionParser();
    	
		parser.accepts("help");
    	parser.accepts("p").withRequiredArg().ofType(String.class);
		parser.accepts("prefix").withRequiredArg().ofType(String.class);
    	parser.accepts("v");
		parser.accepts("verbose");
		
		try {
			options = parser.parse(args);
		}
		catch (Exception e) {
			System.err.println("Error: Invalid options (please use --help for a list): " + e.getMessage());
			return 1;
		}
		
		List<String> l = options.nonOptionArguments();
		
		
		// Parse the command-line options: Options & help
		
		if (options.has("help") || l.size() < 1 || l.size() > 2) {
			System.err.println(PROGRAM_LONG_NAME);
			System.err.println("");
			System.err.println("Usage: " + PROGRAM_NAME + " " + tool + " [OPTIONS] INPUT.fgf [OUTPUT_DIR]");
			System.err.println("");
			System.err.println("Options:");
			System.err.println("  --help               Print this help");
			System.err.println("  --prefix, -p PREFIX  Set the file name prefix for the output .csv files");
			System.err.println("  --verbose, -v        Verbose (print progress)");
			return options.has("help") ? 0 : 1;
		}
		
		boolean verbose = options.has("v") || options.has("verbose");
		
		String prefix = null;
		if (options.has("p") || options.has("prefix")) {
			prefix = options.valueOf(options.has("p") ? "p" : "prefix").toString();
		}
		
		
		// Parse the command-line options: Non-optional arguments
    	
    	String inputFile = l.get(0);
    	String outputDir = l.size() > 1 ? l.get(1) : null;
    	
    	if (!inputFile.endsWith(".fgf")) {
    		System.err.println("Error: The input file needs to have the .fgf extension");
    		System.exit(1);
    	}
    	
    	File file = new File(inputFile);
    	File dir  = outputDir != null ? new File(outputDir) : new File(file.getParentFile(), file.getName() + "-dex-csvs");
    	
    	if (dir.exists() && !dir.isDirectory()) {
    		System.err.println("Error: The output directory is not a directory -- " + outputDir);
    		System.exit(1);
    	}
   	
    	if (prefix == null) {
    		prefix = file.getName();
    		if (prefix.endsWith(".fgf")) {
    			prefix = prefix.substring(0, prefix.length() - 4);
    		}
    	}
    	
    	
    	// Tool
    	
    	
    	if (verbose) System.err.print("Converting:");
    	FGF2DexCSV.convert(file, dir, prefix, verbose ? new GraphReaderProgressListener() : null);
    	if (verbose) System.err.println();

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
			System.err.println("Error: Invalid options (please use --help for a list): " + e.getMessage());
			return 1;
		}
		
		List<String> l = options.nonOptionArguments();
		
		
		// Parse the command-line options: Options & help
		
		if (options.has("help") || l.size() != 2) {
			System.err.println(PROGRAM_LONG_NAME);
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
    	GraphML2FGF.convert(fin, out, verbose ? new GraphReaderProgressListener() : null);
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
			System.err.println("Error: Invalid options (please use --help for a list): " + e.getMessage());
			return 1;
		}
		
		List<String> l = options.nonOptionArguments();
		
		
		// Parse the command-line options: Options & help
		
		if (options.has("help") || l.size() != 1) {
			System.err.println(PROGRAM_LONG_NAME);
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
				public void vertexTypeStart(VertexType type, long count) {
					System.out.println("" + count + " node" + (count == 1 ? "" : "s")
							+ " of type " + ("".equals(type.getName()) ? "<default>" : type.getName()));
				}
				
				@Override
				public void vertexTypeEnd(VertexType type, long count) {
				}
				
				@Override
				public void vertex(long id, VertexType type, Map<PropertyType, Object> properties) {
				}
				
				@Override
				public void propertyType(PropertyType type) {
					System.out.println("Property " + type.getName() + " of type " + FGFTypes.toString(type.getType()));
				}
				
				@Override
				public void edgeTypeStart(EdgeType type, long count) {
					System.out.println("" + count + " edge" + (count == 1 ? "" : "s")
							+ " of type " + ("".equals(type.getName()) ? "<default>" : type.getName()));
				}
				
				@Override
				public void edgeTypeEnd(EdgeType type, long count) {
				}
				
				@Override
				public void edge(long id, long head, long tail, EdgeType type, Map<PropertyType, Object> properties) {
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
    static class GraphReaderProgressListener implements GraphProgressListener {
    	
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
    	public void graphProgress(int vertices, int edges) {
    		
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
