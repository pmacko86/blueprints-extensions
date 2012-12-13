package com.tinkerpop.blueprints.extensions.io.fgf.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import com.tinkerpop.blueprints.extensions.io.fgf.FGFFileWriter;
import com.tinkerpop.blueprints.extensions.io.fgf.tools.FGFTool.GraphReaderProgressListener;


/**
 * Fast Graph Format: Importer from a pairs file
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class Pairs2FGF {
	
	private static boolean verbose = false;
	
	
	/**
	 * Print the usage info for the tool
	 * 
	 * @param tool the tool name
	 */
	private static void usage(String tool) {
		System.err.println(FGFTool.PROGRAM_LONG_NAME);
		System.err.println("");
		System.err.println("Usage: " + FGFTool.PROGRAM_NAME + " " + tool + " [OPTIONS] FILE");
		System.err.println("");
		System.err.println("Options:");
		System.err.println("  --help                            Print this help");
		System.err.println("  --node-properties, -p KEY,KEY,... Specify which node properties to load");
		System.err.println("  --node-properties-file, -P FILE   Specify the node properties file");
		System.err.println("  --output, -o FILE                 Specify the output file");
		System.err.println("  --separator, -s SEP               Specify the separator for the input file as a regular");
		System.err.println("                                    expression");
		System.err.println("  --verbose, -v                     Verbose (print progress)");
		System.err.println("");
		System.err.println("Input File Format -- example:");
		System.err.println("  # A sample graph from http://snap.stanford.edu/data/");
		System.err.println("  # FromNodeId\tToNodeId");
		System.err.println("  0\t1");
		System.err.println("  0\t2");
		System.err.println("  0\t3");
		System.err.println("  0\t4");
		System.err.println("  0\t5");
		System.err.println("  1\t0");
		System.err.println("  1\t2");
		System.err.println("  1\t4");
		System.err.println("  ...");
		System.err.println("");
		System.err.println("Node Property File Format -- example:");
		System.err.println("  # A sample property file");
		System.err.println("  Total items: 548552");
		System.err.println("  ");
		System.err.println("  Id:   0");
		System.err.println("  ASIN: 0771044445");
		System.err.println("  discontinued product");
		System.err.println("  ");
		System.err.println("  Id:   1");
		System.err.println("  ASIN: 0827229534");
		System.err.println("  title: Patterns of Preaching: A Sermon Sampler");
		System.err.println("  group: Book");
		System.err.println("  ");
		System.err.println("  Id:   2");
		System.err.println("  ASIN: 0738700797");
		System.err.println("  title: Candlemas: Feast of Flames");
		System.err.println("  group: Book");
		System.err.println("  ...");
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
    	
    	// Parse the command-line options
    	
		OptionSet options;
		OptionParser parser = new OptionParser();
    	
		parser.accepts("help");
    	parser.accepts("p").withRequiredArg().ofType(String.class);
		parser.accepts("node-properties").withRequiredArg().ofType(String.class);
    	parser.accepts("P").withRequiredArg().ofType(String.class);
		parser.accepts("node-properties-file").withRequiredArg().ofType(String.class);
    	parser.accepts("o").withRequiredArg().ofType(String.class);
		parser.accepts("output").withRequiredArg().ofType(String.class);
	   	parser.accepts("s").withRequiredArg().ofType(String.class);
		parser.accepts("separator").withRequiredArg().ofType(String.class);
    	parser.accepts("v");
		parser.accepts("verbose");
 		
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
			usage(tool);
			return options.has("help") ? 0 : 1;
		}
		
		String separator = "[ \t,;]+";
		if (options.has("s") || options.has("separator")) {
			separator = (String) (options.has("s") ? options.valueOf("s") : options.valueOf("separator"));
		}
		
		String nodePropertiesSeparator = ":[ \t]*";

		
		// The input file
		
		if (options.nonOptionArguments().size() > 1) {
			System.err.println("Too many arguments (please use --help)");
			return 1;
		}
		
		File inputFile = new File(options.nonOptionArguments().get(0));
		
		if (!inputFile.exists()) {
    		System.err.println("Error: The input file does not exist.");
    		return 1;
    	}
		
		
		// The node properties and the node properties file
		
		String nodePropertiesFileName = null;
		File nodePropertiesFile = null;
		Vector<String> nodePropertiesVector = new Vector<String>();
		HashMap<String, Integer> nodePropertiesToIndex = new HashMap<String, Integer>();
		
		if (options.has("P") || options.has("node-properties-file")) {
			
			nodePropertiesFileName = (String) (options.has("P") ? options.valueOf("P") : options.valueOf("node-properties-file"));
			nodePropertiesFile = new File(nodePropertiesFileName);
			
			if (options.has("p") || options.has("node-properties")) {
				for (Object a : options.has("p") ? options.valuesOf("p") : options.valuesOf("node-properties")) {
					for (String s : a.toString().split("[,]")) {
						if (!"".equals(s) && !nodePropertiesVector.contains(s)) {
							nodePropertiesToIndex.put(s, nodePropertiesVector.size());
							nodePropertiesVector.add(s);
						}
					}
				}
			}
			else {
				System.err.println("Error: A list of node properties must be specified.");
	    		return 1;
			}
			
			if (!nodePropertiesFile.exists()) {
	    		System.err.println("Error: The node properties file does not exist.");
	    		return 1;
	    	}
		}
		else {
			if (options.has("p") || options.has("node-properties")) {
				System.err.println("Error: A node properties file must be specified.");
	    		return 1;
			}
		}

		
		// The output file
		
		String outputFileName = null;
		if (options.has("o") || options.has("output")) {
			outputFileName = (String) (options.has("o") ? options.valueOf("o") : options.valueOf("output"));
		}
		else {
			int s = inputFile.getName().lastIndexOf('.');
			if (s > 0) {
				outputFileName = inputFile.getName().substring(0, s) + ".fgf";
			}
		}
		
	    if (!outputFileName.endsWith(".fgf")) {
	    	System.err.println("Error: The output file needs to have the .fgf extension.");
	    	return 1;
		}
	    
	    File outputFile = new File(outputFileName);
		
		
		// Convert

		try {
			
			GraphReaderProgressListener l;
			
			BufferedReader in;
			String line;
			
			int lineNo = 0;
			int numVertices = 0;
			int numEdges = 0;
		

			/*
			 * Read the node properties file
			 */
			
			Object[][] nodeProperties = new Object[nodePropertiesFile == null ? 0 : 10 * 1000000][];
			
			lineNo = 0;
			numVertices = 0;
			numEdges = 0;
			
			if (nodePropertiesFile != null && !nodePropertiesVector.isEmpty()) {
				
				l = verbose ? new GraphReaderProgressListener() : null;
				if (verbose) System.err.print("Properties:");
				if (verbose) l.graphProgress((int) 0, (int) 0);

				in = new BufferedReader(new FileReader(nodePropertiesFile));
				
				boolean beginning = true;
				int sectionId = -1;
				int numSinceListenerCallback = 0;
				
				while ((line = in.readLine()) != null) {
					
					if (verbose) {
						if (numSinceListenerCallback >= 10000) {
							l.graphProgress(numVertices, numEdges);
							numSinceListenerCallback = 0;
						}
					}
					
					
					// Preprocess the line, skipping empty lines, commented-out lines, and the file header
					
					lineNo++;
					
					int contentStart;
					for (contentStart = 0; contentStart < line.length(); contentStart++) {
						if (!Character.isWhitespace(line.charAt(contentStart))) {
							if (contentStart > 0) line = line.substring(contentStart);
							break;
						}
					}
					
					if (line.startsWith("#")) continue;
					
					if (line.length() == 0) {
						beginning = false;
						sectionId = -1;
						continue;
					}
					
					if (line.startsWith("Id:")) {
						beginning = false;
					}
					
					if (beginning) continue;
					
					
					// Section start
					
					if (line.startsWith("Id:")) {
						String[] strFields = line.split(nodePropertiesSeparator, 2);
						if (strFields.length != 2) {
							throw new RuntimeException("Error: Error on line " + lineNo
									+ " of the node properties file -- no Id is specified");
						}
						
						if (sectionId >= 0) {
							throw new RuntimeException("Error: Error on line " + lineNo
									+ " of the node properties file -- a blank line must preceed a new Id key");
						}
						
						sectionId = Integer.parseInt(strFields[1]);
						if (sectionId < 0) {
							throw new RuntimeException("Error: Error on line " + lineNo
									+ " of the node properties file -- a negative Id");
						}
						
						if (sectionId > nodeProperties.length) {
							int m = sectionId;
							int newSize = m < Integer.MAX_VALUE / 4 ? 2 * m : Integer.MAX_VALUE / 2;
							if (m >= newSize) {
								throw new RuntimeException("Error: Error on line " + lineNo
										+ " of the node properties file -- too large Id");
							}
							Object[][] a = new Object[newSize][];
							System.arraycopy(nodeProperties, 0, a, 0, nodeProperties.length);
							nodeProperties = a;
							System.runFinalization();
							System.gc();
						}
						
						nodeProperties[sectionId] = new Object[nodePropertiesVector.size()];
						
						numVertices++;
						numSinceListenerCallback++;
						continue;
					}
					
					
					// A simple single-value properties (we currently do not support multi-valued properties)
					
					if (sectionId < 0) {
						throw new RuntimeException("Error: Error on line " + lineNo
								+ " of the node properties file -- does not belong to any section with a specified Id");
					}
					
					for (int i = 0; i < nodePropertiesVector.size(); i++) {
						if (line.startsWith(nodePropertiesVector.get(i))) {
							if (line.startsWith(nodePropertiesVector.get(i) + ":")) {
								String[] strFields = line.split(nodePropertiesSeparator, 2);
								if (strFields.length != 2) {
									throw new RuntimeException("Error: Error on line " + lineNo
											+ " of the node properties file -- no value for key " + nodePropertiesVector.get(i));
								}
								Object[] data = nodeProperties[sectionId];						
								data[i] = strFields[1];
							}
						}
					}
				}
	
				in.close();
				
				
				// Finish by checking if any properties parse as integers
				
				int[] temp = new int[nodeProperties.length];
				
				for (int pi = 0; pi < nodePropertiesVector.size(); pi++) {
					boolean ok = true;
					
					for (int i = 0; i < nodeProperties.length; i++) {
						Object[] p = nodeProperties[i];
						if (p == null) continue;
						if (p[pi] == null) continue;
						
						try {
							int n = Integer.parseInt((String) p[pi]);
							if (!((String) p[pi]).equals("" + n)) {
								ok = false;
								break;
							}
							temp[i] = n;
						}
						catch (Exception e) {
							ok = false;
							break;
						}
					}
					
					if (ok) {
						for (int i = 0; i < nodeProperties.length; i++) {
							Object[] p = nodeProperties[i];
							if (p != null && p[pi] != null) p[pi] = temp[i];
						}
					}
				}
				
				
				if (verbose) l.graphProgress(numVertices, numEdges);
				if (verbose) System.err.println();
			}
		
			
			/*
			 * Read the pairs file and write out the FGF file
			 */
			
			in = new BufferedReader(new FileReader(inputFile));
			FGFFileWriter writer = new FGFFileWriter(outputFile);
			
			l = verbose ? new GraphReaderProgressListener() : null;
			if (verbose) System.err.print("Converting:");
			if (verbose) l.graphProgress((int) 0, (int) 0);
			
			
			// Begin the main loop
			
			lineNo = 0;
			numVertices = 0;
			numEdges = 0;
			int[] fields = new int[2];
			
			long[] nodes = new long[10 * 1000000];
			for (int i = 0; i < nodes.length; i++) nodes[i] = -1;
			
			HashMap<String, Object> propertyMap = new HashMap<String, Object>();
			
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
				
				int from = fields[0] = Integer.parseInt(strFields[0]);
				int to   = fields[1] = Integer.parseInt(strFields[1]);
				
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
					long[] a = new long[newSize];
					for (int i = nodes.length; i < newSize; i++) {
						a[i] = -1;
					}
					System.arraycopy(nodes, 0, a, 0, nodes.length);
					nodes = a;
					System.runFinalization();
					System.gc();
				}
				
				for (int f : fields) {
					long n = nodes[f];
					if (n < 0) {
						Object[] properties = f >= nodeProperties.length ? null : nodeProperties[f];
						propertyMap.clear();
						if (properties != null) {
							for (int i = 0; i < nodePropertiesVector.size(); i++) {
								if (properties[i] != null) {
									propertyMap.put(nodePropertiesVector.get(i), properties[i]);
								}
							}
						}
						n = nodes[f] = writer.writeVertex(propertyMap);
						numVertices++;
					}
				}
				
				writer.writeEdge(nodes[from] /* tail */, nodes[to] /* head */, "", null);
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
			
			if (verbose) System.err.print("Finalizing: ");
			writer.close();
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

}
