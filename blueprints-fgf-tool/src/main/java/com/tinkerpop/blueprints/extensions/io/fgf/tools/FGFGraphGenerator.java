package com.tinkerpop.blueprints.extensions.io.fgf.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
		System.err.println("       " + FGFTool.PROGRAM_NAME + " generate [OPTIONS] SPEC_FILE_NAME");
		System.err.println("");
		System.err.println("Options:");
		System.err.println("  --help               Print this help");
		System.err.println("  --labels, -l L,L...  Specify edge labels (will be sampled uniformly)");
		System.err.println("  --output, -o FILE    Specify the output file");
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
		
		
		// Parse the command-line options: Some Options & help
		
		if (options.has("help") || options.nonOptionArguments().isEmpty()) {
			usage();
			return options.has("help") ? 0 : 1;
		}
		
		verbose = options.has("v") || options.has("verbose");
		
		
		// Model and model arguments
    	
		ArrayList<String> modelArgs = new ArrayList<String>(options.nonOptionArguments());
		String model = modelArgs.remove(0);
		FGFGraphGeneratorSpecs specs = null;
		boolean fromFile = false;
    	
		try {
	    	if ("barabasi".equals(model)) {
	    		specs = createBarabasiSpecs(modelArgs.toArray(new String[0]));
	    	}
	    	else {
	    		File f = new File(model);
	    		if (f.exists() && f.isFile()) {
	    			if (!modelArgs.isEmpty()) { 
	    				System.err.println("Error: Too many arguments (please use --help for a list).");
	    				return 1;
	    			}
	    			fromFile = true;
	    			specs = FGFGraphGeneratorSpecs.loadFromXML(f);
	    		}
	    		else {
	    			System.err.println("Error: Invalid graph model or a spec file (please use --help for a list).");
	    			return 1;
	    		}
	    	}
		}
		catch (Exception e) {
			if (e.getMessage().startsWith("Error: ")) {
				System.err.println(e.getMessage());
				return 1;
			}
			throw e;
		}
    	
    	assert specs != null;
    	
    	
    	// Additional options for specs
		
		String outputFile;
		if (options.has("o") || options.has("output")) {
			outputFile = options.valueOf(options.has("o") ? "o" : "output").toString();
	    	if (!outputFile.endsWith(".fgf")) {
	    		System.err.println("Error: The output file needs to have the .fgf extension.");
	    		return 1;
	    	}
		}
		else {
			if (specs.getDefaultOutputFile() == null) {
				System.err.println("Error: The option -o/--output is required, unless it is specified in the specs file.");
				return 1;
			}
			else {
				outputFile = specs.getDefaultOutputFile();
		    	if (!outputFile.endsWith(".fgf")) {
		    		System.err.println("Error: The output file needs to have the .fgf extension.");
		    		return 1;
		    	}
			}
		}
		
		if (options.has("l") || options.has("labels")) {
			
			if (fromFile) {
    			System.err.println("Error: The option -l/--lavels is unsupported if the specs are loaded from a file.");
    			return 1;
			}
			
			if (options.has("l")) {
				for (Object a : options.valuesOf("l")) {
					for (String s : a.toString().split("[, \t\n]")) {
						if (!"".equals(s)) specs.addEdgeLabel(s);
						if ("<default>".equals(s)) specs.addEdgeLabel("");
					}
				}
			}
			
			if (options.has("labels")) {
				for (Object a : options.valuesOf("labels")) {
					for (String s : a.toString().split("[, \t\n]")) {
						if (!"".equals(s)) specs.addEdgeLabel(s);
						if ("<default>".equals(s)) specs.addEdgeLabel("");
					}
				}
			}
		}
		
		
		// Generate

		try {
			generate(specs, new File(outputFile));
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
     * Create specs for the Barabasi model
     * 
     * @param args the model arguments
     * @return the specs
     */
    private static FGFGraphGeneratorSpecs createBarabasiSpecs(String[] args) {
    	
    	if (args.length != 2) {
    		throw new RuntimeException("Error: Invalid number of model arguments (please use --help for help).");
    	}
    	
    	FGFGraphGeneratorSpecs specs = new FGFGraphGeneratorSpecs("barabasi");
    	specs.setModelParameter("n", args[0]);
    	specs.setModelParameter("m", args[1]);
    	
    	return specs; 
    }
    
    
    /**
     * Generate the graph
     * 
     * @param specs the specs
     * @param outputFile the output file
     * @throws IOException on I/O error
     */
    private static void generate(FGFGraphGeneratorSpecs specs, File outputFile) throws IOException {
    	
    	assert specs.getModelName() != null;
    	assert specs.getModelParameters() != null;
    	
   	
    	//
    	// Generate the topology
    	//
    	
    	if ("barabasi".equalsIgnoreCase(specs.getModelName())) {
    		barabasi(specs);
    	}
    	else if ("pairs-file".equalsIgnoreCase(specs.getModelName())) {
    		pairsFile(specs);
    	}
    	else {
    		throw new IllegalArgumentException("Unknown model " + specs.getModelName());
    	}
    	
    	
    	// Check that the graph was generated + sanity checks
		
		if (heads == null || tails == null || heads.length != tails.length) {
			throw new IllegalStateException("Error: Internal model generator error.");
		}
		
		
		//
		// Write & randomly assign properties + edge labels
		//
		
		FGFWriter out = new FGFWriter(outputFile);
		
		GraphReaderProgressListener l = verbose ? new GraphReaderProgressListener() : null;
		if (verbose) System.err.print("Writing   :");
		
		List<String> edgeLabels = specs.getEdgeLabels();
		FGFGraphGeneratorSpecs.Distribution edgeLabelsDistribution = specs.getEdgeLabelsDistribution();
		List<FGFGraphGeneratorSpecs.Property> edgeProperties = specs.getEdgeProperties(); 
		List<FGFGraphGeneratorSpecs.Property> vertexProperties = specs.getVertexProperties(); 
		
		HashMap<String, Object> properties = new HashMap<String, Object>();
		
		
		// Vertices
		
		for (long i = 0; i < vertices; i++) {
			
			properties.clear();
			for (FGFGraphGeneratorSpecs.Property p : vertexProperties) {
				properties.put(p.getName(), p.generateValue());
			}
			
			long _i = out.writeVertex("", properties);
			assert i == _i;
			
			if (verbose && i % 100000 == 0) l.graphProgress((int) i, (int) 0);
		}
		
		
		// Edges
		
		for (long i = 0; i < heads.length; i++) {
			
			String type = "";
			if (!edgeLabels.isEmpty()) {
				type = edgeLabels.get(edgeLabelsDistribution.randomInt(edgeLabels.size())); 
			}
			
			properties.clear();
			for (FGFGraphGeneratorSpecs.Property p : edgeProperties) {
				properties.put(p.getName(), p.generateValue());
			}
			
			out.writeEdge(heads[(int) i], tails[(int) i], type, properties);
			
			if (verbose && i % 100000 == 0) l.graphProgress((int) vertices, (int) i);
		}
		
		
		// Finalize
		
		if (verbose) l.graphProgress((int) vertices, (int) heads.length);
		if (verbose) System.err.println();
		
		if (verbose) System.err.print("Finalizing: ");
		out.close();
		if (verbose) System.err.println("done");
    }

    
    /**
     * Barabasi graph generator
     * 
     * @param specs the generator specs
     */
    private static void barabasi(FGFGraphGeneratorSpecs specs) {
    	
       	assert specs.getModelName().equalsIgnoreCase("barabasi");
       	
    	
    	// Get the arguments
    	
    	if (specs.getModelParameters().size() != 2) {
    		throw new RuntimeException("Error: Invalid number of model arguments (please use --help for help).");
    	}
    	
    	String s_n = specs.getModelParameters().get("n");
    	String s_m = specs.getModelParameters().get("m");
    	if (s_n == null || s_m == null) {
    		throw new RuntimeException("Error: Both n and m need to be specified.");
    	}
    	
    	long n = Long.parseLong(s_n);
    	long m = Long.parseLong(s_m);
    	int zeroAppeal = 8;
    	
    	if (n < 1) throw new RuntimeException("Error: n < 1");
    	if (m < 1) throw new RuntimeException("Error: m < 1");
    	
    	if (m * (n - 1) >= Integer.MAX_VALUE) {
    		throw new RuntimeException("Error: Too many edges!");
    	}
    	
    	
    	// Initialize	
       	
       	Random random = new Random();
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
    			// New edge: o ---> i
    			heads[edge_i] = i;
    			tails[edge_i] = o;
    			edge_i++;
    		}
    		
			if (verbose && (i == 1 || i % 100000 == 0)) l.graphProgress((int) vertices, (int) i);
    	}
    	
    	
    	// Finish
    	
    	assert edge_i == heads.length && edge_i == tails.length;
		
		if (verbose) l.graphProgress((int) vertices, (int) heads.length);
		if (verbose) System.err.println();
    }

    
    /**
     * Load the graph topology from a pairs file
     * 
     * @param specs the generator specs
     */
    private static void pairsFile(FGFGraphGeneratorSpecs specs) {
    	
       	assert specs.getModelName().equalsIgnoreCase("pairs-file");
       	
    	
    	// Get the arguments
    	
    	if (specs.getModelParameters().size() != 1) {
    		throw new RuntimeException("Error: Invalid number of model arguments (please use --help for help).");
    	}
    	
    	String fileName = specs.getModelParameters().get("file");
    	if (fileName == null) {
    		throw new RuntimeException("Error: No \"file\" is specified.");
    	}
    	
    	File file = new File(fileName);
    	if (!file.exists()) {
    		throw new RuntimeException("Error: The specified pairs file does not exist.");
    	}
    	
    	
    	// Read the file
    	
    	try {
			
    		BufferedReader in = new BufferedReader(new FileReader(file));
			
    		GraphReaderProgressListener l = verbose ? new GraphReaderProgressListener() : null;
			if (verbose) System.err.print("Importing :");
			if (verbose) l.graphProgress((int) 0, (int) 0);
			
			
			// Begin the main loop
			
			String line;
			String separator = "[ \t,]+";
			int lineNo = 0;
			int numVertices = 0;
			int numEdges = 0;
			
			int[] nodes = new int[10 * 1000000];
			for (int i = 0; i < nodes.length; i++) nodes[i] = -1;
			
			ArrayList<int[]> edges = new ArrayList<int[]>();
			
			while ((line = in.readLine()) != null) {
				
				
				// Preprocess the line, skipping empty lines and commented-out lines
				
				lineNo++;
				
				int contentStart;
				for (contentStart = 0; contentStart < line.length(); contentStart++) {
					if (!Character.isWhitespace(line.charAt(contentStart))) {
						if (contentStart > 0) line = line.substring(contentStart);
						break;
					}
				}
				
				if (line.length() == 0 || line.startsWith("#")) continue;
				
				
				// Parse the pair
				
				String[] strFields = line.split(separator);
				if (strFields.length != 2) {
					throw new RuntimeException("Error: Error on line " + lineNo + " of the input file -- invalid number of fields");
				}
				
				int from = Integer.parseInt(strFields[0]);
				int to   = Integer.parseInt(strFields[1]);
				
				if (from < 0 || to < 0) {
					throw new RuntimeException("Error: Error on line " + lineNo + " of the input file -- a negative node ID");
				}
				
				
				// Look up the nodes in the translation dictionary, creating them if they are not there
				
				if (Math.max(from, to) >= nodes.length) {
					int m = Math.max(from, to);
					int newSize = m < Integer.MAX_VALUE / 4 ? 2 * m : Integer.MAX_VALUE / 2;
					if (m >= newSize) {
						throw new RuntimeException("Error: Error on line " + lineNo + " of the input file -- too large node ID");
					}
					int[] a = new int[newSize];
					for (int i = nodes.length; i < newSize; i++) {
						a[i] = -1;
					}
					System.arraycopy(nodes, 0, a, 0, nodes.length);
					nodes = a;
					System.runFinalization();
					System.gc();
				}
				
				if (nodes[from] < 0) nodes[from] = numVertices++;
				if (nodes[to  ] < 0) nodes[to  ] = numVertices++;
				
				
				// Create the edge
				
				edges.add(new int[] { nodes[to] /* head */, nodes[from] /* tail */ });
				numEdges++;
				
				
				// Listener
				
				if (verbose) {
					if ((numVertices + numEdges) % 100000 == 0) {
						l.graphProgress(numVertices, numEdges);
					}
				}
			}
			
			
			// Finalize
			
			if (verbose) l.graphProgress(numVertices, numEdges);
			if (verbose) System.err.println();
			
			in.close();
			
			heads = new int[edges.size()];
			tails = new int[edges.size()];
			vertices = numVertices;
			
			int i = 0;
			for (int[] e : edges) {
				heads[i] = e[0];
				tails[i] = e[1];
				i++;
			}
    	}
    	catch (IOException e) {
    		throw new RuntimeException(e);
    	}
    }
}
