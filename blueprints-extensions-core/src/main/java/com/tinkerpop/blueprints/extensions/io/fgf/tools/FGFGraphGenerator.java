package com.tinkerpop.blueprints.extensions.io.fgf.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import com.tinkerpop.blueprints.extensions.io.fgf.FGFWriter;
import com.tinkerpop.blueprints.extensions.io.fgf.tools.FGFTool.GraphReaderProgressListener;


/**
 * Fast Graph Format: Graph generator
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class FGFGraphGenerator {
	
	private static boolean verbose = false;
	
	private static int[] heads = null;
	private static int[] tails = null;
	private static long vertices = 0;
	
	
	/**
	 * Print the usage info for the tool
	 */
	private static void usage() {
		System.err.println(FGFTool.PROGRAM_LONG_NAME);
		System.err.println("");
		System.err.println("Usage: " + FGFTool.PROGRAM_NAME + " generate [OPTIONS] MODEL MODEL_PARAMS...");
		System.err.println("");
		System.err.println("Options:");
		System.err.println("  --help               Print this help");
		System.err.println("  --labels, -l L,L...  Specify edge labels (will be sampled uniformly)");
		System.err.println("  --output, -o FILE    Specify the output file (required)");
		System.err.println("  --verbose, -v        Verbose (print progress)");
		System.err.println("");
		System.err.println("Models:");
		System.err.println("  barabasi N M         Barabasi with N nodes, M node degree");
	}

    
    /**
     * Tool: Generate a graph and save it as .fgf
     * 
     * @param tool the tool name
     * @param args the command-line arguments
     * @return the exit code
     * @throws IOException on I/O error
     */
    static int run(String tool, String[] args) throws Exception {
    	
    	
    	// Parse the command-line options
    	
		OptionSet options;
		OptionParser parser = new OptionParser();
    	
		parser.accepts("help");
    	parser.accepts("l").withRequiredArg().ofType(String.class);
		parser.accepts("labels").withRequiredArg().ofType(String.class);
    	parser.accepts("o").withRequiredArg().ofType(String.class);
		parser.accepts("output").withRequiredArg().ofType(String.class);
    	parser.accepts("v");
		parser.accepts("verbose");
		
		try {
			options = parser.parse(args);
		}
		catch (Exception e) {
			System.err.println("Invalid options (please use --help for a list): " + e.getMessage());
			return 1;
		}
		
		
		// Parse the command-line options: Options & help
		
		if (options.has("help") || options.nonOptionArguments().isEmpty()) {
			usage();
			return options.has("help") ? 0 : 1;
		}
		
		ArrayList<String> edgeLabels = new ArrayList<String>();
		if (options.has("l") || options.has("labels")) {
			
			if (options.has("l")) {
				for (Object a : options.valuesOf("l")) {
					for (String s : a.toString().split("[, \t\n]")) {
						if (!"".equals(s)) edgeLabels.add(s);
						if ("<default>".equals(""));
					}
				}
			}
			
			if (options.has("labels")) {
				for (Object a : options.valuesOf("labels")) {
					for (String s : a.toString().split("[, \t\n]")) {
						if (!"".equals(s)) edgeLabels.add(s);
						if ("<default>".equals(""));
					}
				}
			}
		}
		
		String outputFile;
		if (options.has("o") || options.has("output")) {
			outputFile = options.valueOf(options.has("o") ? "o" : "output").toString();
	    	if (!outputFile.endsWith(".fgf")) {
	    		System.err.println("Error: The output file needs to have the .fgf extension");
	    		return 1;
	    	}
		}
		else {
			System.err.println("Error: The option -o/--output is required");
			return 1;
		}
		
		verbose = options.has("v") || options.has("verbose");
		
		
		// Model and model arguments
    	
		ArrayList<String> modelArgs = new ArrayList<String>(options.nonOptionArguments());
		String model = modelArgs.remove(0);
    	
		try {
	    	if ("barabasi".equals(model)) {
	    		barabasi(modelArgs.toArray(new String[0]));
	    	}
	    	else {
	    		System.err.println("Error: Invalid graph model (please use --help for a list)");
				return 1;
	    	}
		}
		catch (Exception e) {
			if (e.getMessage().startsWith("Error: ")) {
				System.err.println(e.getMessage());
				return 1;
			}
			throw e;
		}
		
		if (heads == null || tails == null || heads.length != tails.length) {
			System.err.println("Error: Internal model generator error");
			return 1;
		}
		
		
		// Write & randomly assign types
		
		Random random = new Random();
		FGFWriter out = new FGFWriter(new File(outputFile));
		
		GraphReaderProgressListener l = verbose ? new GraphReaderProgressListener() : null;
		if (verbose) System.err.print("Writing   :");
		
		for (long i = 0; i < vertices; i++) {
			
			long _i = out.writeVertex("", null);
			assert i == _i;
			
			if (verbose && i % 100000 == 0) l.graphProgress((int) i, (int) 0);
		}
		
		for (long i = 0; i < heads.length; i++) {
			
			String type = "";
			if (!edgeLabels.isEmpty()) type = edgeLabels.get(random.nextInt(edgeLabels.size())); 
			
			out.writeEdge(heads[(int) i], tails[(int) i], type, null);
			
			if (verbose && i % 100000 == 0) l.graphProgress((int) vertices, (int) i);
		}
		
		if (verbose) l.graphProgress((int) vertices, (int) heads.length);
		if (verbose) System.err.println();
		
		if (verbose) System.err.print("Finalizing: ");
		out.close();
		if (verbose) System.err.println("done");

    	return 0;
    }
    
    
    /**
     * Barabasi graph generator
     * 
     * @param args the model arguments
     */
    private static void barabasi(String[] args) {
    	
    	Random random = new Random();
    	
    	
    	// Get the arguments
    	
    	if (args.length != 2) {
    		throw new RuntimeException("Error: Invalid number of model arguments (please use --help for help)");
    	}
    	
    	long n = Long.parseLong(args[0]);
    	long m = Long.parseLong(args[1]);
    	int zeroAppeal = 8;
    	
    	if (n < 1) throw new RuntimeException("Error: n < 1");
    	if (m < 1) throw new RuntimeException("Error: m < 1");
    	
    	if (m * (n - 1) >= Integer.MAX_VALUE) {
    		throw new RuntimeException("Error: Too many edges!");
    	}
    	
    	
    	// Initialize
		
		GraphReaderProgressListener l = verbose ? new GraphReaderProgressListener() : null;
   	
		try {
	    	vertices = n;
	    	heads = new int[(int) (m * (n - 1))];
	    	tails = new int[(int) (m * (n - 1))];
		}
		catch (OutOfMemoryError e) {
			throw new RuntimeException("Error: Out of memory -- need at least "
					+ Math.round(Math.ceil((((m * (n - 1)) * 8) / 1048576.0 + 256) / 1024.0)) + " GB "
					+ "(use the +help option to see how to set it)");
		}
    	
    	
    	// Generate the edges
		
		if (verbose) System.err.print("Generating:");
    	
    	int edge_i = 0;
    	int[] otherVertices = new int[(int) m];
    	
    	for (int i = 1; i < n; i++) {
    		
    		// Get an array of m vertices to connect to, weigh by degree + zero appeal

    		// Original code from GraphDB-Bench:
    		// Evaluator evaluator = new EvaluatorDegree(1, 8);
    		// otherVertices = StatisticsHelper
    		// 		.getSampleVertexIds(graph, evaluator, m);

    		for (int j = 0; j < m; j++) {

    			// Account for the zero-appeal parameter

    			long zeroAppealPts = (1 + i) * zeroAppeal;
    			long totalPts = zeroAppealPts + 2 * edge_i;
    			long r = (long) (random.nextDouble() * totalPts);

    			if (r < zeroAppealPts) {
    				otherVertices[j] = (int) (r / zeroAppeal);
    				assert otherVertices[j] < i;
    			}
    			else {
    				long x = r - zeroAppealPts;
    				int e = (int) (x / 2);
    				otherVertices[j] = (x & 1) == 0 ? heads[e] : tails[e];
    			}
    		}


    		// Create the edges

    		for (int o : otherVertices) {
    			heads[edge_i] = o;
    			tails[edge_i] = i;
    			edge_i++;
    		}
    		
			if (verbose && (i == 1 || i % 100000 == 0)) l.graphProgress((int) vertices, (int) i);
    	}
    	
    	
    	// Finish
    	
    	assert edge_i == heads.length && edge_i == tails.length;
		
		if (verbose) l.graphProgress((int) vertices, (int) heads.length);
		if (verbose) System.err.println();
    }
}
