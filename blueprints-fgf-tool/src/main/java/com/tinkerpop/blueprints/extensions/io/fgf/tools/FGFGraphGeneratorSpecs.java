package com.tinkerpop.blueprints.extensions.io.fgf.tools;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import com.tinkerpop.blueprints.extensions.io.fgf.FGFTypes;


/**
 * Fast Graph Format: Graph generator specifications & configuration
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class FGFGraphGeneratorSpecs {
	
	private static ThreadLocal<Random> random = new ThreadLocal<Random>() {
		@Override protected Random initialValue() { return new Random(); }
	};
	
	private final static String XML_ROOT  = "graph-specs";
	private final static String XML_MODEL = "model";
	private final static String XML_EDGES = "edges";
	private final static String XML_EDGES_LABELS = "labels";
	private final static String XML_EDGES_LABELS_LABEL = "label";
	private final static String XML_VERTICES = "vertices";
	private final static String XML_PROPERTIES = "properties";
	private final static String XML_PROPERTIES_PROPERTY = "property";
	private final static String XML_DISTRIBUTION = "distribution";
	private final static String XML_INCLUDE = "include";
	private final static String XML_INCLUDED = "included";
	
	private String defaultOutputFile = null;
	
	private String modelName = null;
	private HashMap<String, String> modelParameters = new HashMap<String, String>();
	
	private List<String> edgeLabels = new ArrayList<String>();
	private Distribution edgeLabelsDistribution = new Distribution.Uniform();
	
	private List<Property> edgeProperties = new ArrayList<Property>();
	private List<Property> vertexProperties = new ArrayList<Property>();
	
	
	/**
	 * Create an instance of class FGFGraphGeneratorSpec
	 */
	private FGFGraphGeneratorSpecs() {
		//
	}
	
	
	/**
	 * Create an instance of class FGFGraphGeneratorSpec
	 * 
	 * @param modelName the model name
	 */
	public FGFGraphGeneratorSpecs(String modelName) {
		this.modelName = modelName;
	}


	/**
	 * Return the default output file
	 * 
	 * @return the default output file or null if not specified
	 */
	public String getDefaultOutputFile() {
		return defaultOutputFile;
	}


	/**
	 * Return the model name
	 * 
	 * @return the model name
	 */
	public String getModelName() {
		return modelName;
	}


	/**
	 * Return the map of the model parameters
	 * 
	 * @return the model parameters
	 */
	public Map<String, String> getModelParameters() {
		return modelParameters;
	}


	/**
	 * Return the list of edge labels
	 * 
	 * @return the edge labels
	 */
	public List<String> getEdgeLabels() {
		return edgeLabels;
	}


	/**
	 * Return the distribution of the edge labels
	 * 
	 * @return the edge labels distribution
	 */
	public Distribution getEdgeLabelsDistribution() {
		return edgeLabelsDistribution;
	}


	/**
	 * Return the edge properties
	 * 
	 * @return the edge properties
	 */
	public List<Property> getEdgeProperties() {
		return edgeProperties;
	}


	/**
	 * Return the vertex properties
	 * 
	 * @return the vertex properties
	 */
	public List<Property> getVertexProperties() {
		return vertexProperties;
	}
	
	
	/**
	 * Set a model parameter
	 * 
	 * @param key the parameter name
	 * @param value the value
	 */
	public void setModelParameter(String key, String value) {
		modelParameters.put(key, value);
	}
	
	
	/**
	 * Add an edge label. Note that the same label can be added multiple times
	 * in order to skew its probability as desired
	 * 
	 * @param label an edge label
	 */
	public void addEdgeLabel(String label) {
		edgeLabels.add(label);
	}

	
	/**
	 * Read the specs from an XML file
	 * 
	 * @param file the input file
	 * @return the loaded specs
	 * @throws IOException on I/O error
	 */
	public static FGFGraphGeneratorSpecs loadFromXML(File file) throws IOException {
		
		FGFGraphGeneratorSpecs out = new FGFGraphGeneratorSpecs();
		
		try {
			XMLInputFactory inputFactory = XMLInputFactory.newInstance();
			XMLStreamReader reader = inputFactory.createXMLStreamReader(new BufferedInputStream(new FileInputStream(file)));
			loadRootFromXML(reader, file, out);
		}
		catch (XMLStreamException e) {
			throw new IOException(e);
		}
		
		if (out.modelName == null) {
			throw new RuntimeException("The model is not specified.");
		}
		
		return out;
	}

	
	/**
	 * Read the specs from an XML file
	 * 
	 * @param reader the reader
	 * @param file the input file
	 * @param out the specs
	 * @throws XMLStreamException on an XML stream error
	 * @throws IOException on I/O error
	 */
	private static void loadRootFromXML(XMLStreamReader reader, File file, FGFGraphGeneratorSpecs out)
		throws XMLStreamException, IOException {
		
		while (reader.hasNext()) {
			Integer eventType = reader.next();
		
			//
			// Element start
			//
			
			if (eventType.equals(XMLEvent.START_ELEMENT)) {
				String elementName = reader.getName().getLocalPart();
				
				if (elementName.equals(XML_INCLUDED)) {
					continue;
				}

				if (elementName.equals(XML_ROOT)) {
					String outputFile = reader.getAttributeValue(null, "output");
					if (outputFile != null) {
						out.defaultOutputFile = outputFile;
					}
					continue;
				}

				if (elementName.equals(XML_MODEL)) {
					loadModelFromXML(reader, file, out);
					continue;
				}

				if (elementName.equals(XML_EDGES)) {
					loadEdgeSpecsFromXML(reader, file, out);
					continue;
				}

				if (elementName.equals(XML_VERTICES)) {
					loadVertexSpecsFromXML(reader, file, out);
					continue;
				}

				if (elementName.equals(XML_INCLUDE)) {
					String f = reader.getAttributeValue(null, "file");
					if (f == null) {
						throw new RuntimeException("No file specified in <" + elementName + ">.");
					}
					reader.getElementText(); // Make sure that there are no nested elements
					
					File include = f.startsWith("/") ? new File(f) : new File(file.getParentFile(), f);
					
					XMLInputFactory inputFactory = XMLInputFactory.newInstance();
					XMLStreamReader r = inputFactory.createXMLStreamReader(
							new BufferedInputStream(new FileInputStream(include)));
					loadRootFromXML(r, include, out);
					continue;
				}
				
				throw new RuntimeException("Invalid XML element <" + elementName + ">.");
			}
			
			
			//
			// Element finish
			//
			
			if (eventType.equals(XMLEvent.END_ELEMENT)) {
			}
		}
	}
	
	
	/**
	 * Read model information from the XML
	 * 
	 * @param reader the reader
	 * @param file the input file
	 * @param out the specs
	 * @throws XMLStreamException on an XML stream error
	 */
	private static void loadModelFromXML(XMLStreamReader reader, File file, FGFGraphGeneratorSpecs out) throws XMLStreamException {
		
		assert reader.getName().getLocalPart().equals(XML_MODEL);
		
		out.modelName = reader.getAttributeValue(null, "name");
		if (out.modelName == null) {
			throw new RuntimeException("The model name is not specified.");
		}
		
		while (reader.hasNext()) {
			int eventType = reader.next();
		
			
			//
			// Element start
			//
			
			if (eventType == XMLEvent.START_ELEMENT) {
				String elementName = reader.getName().getLocalPart();
				String value = reader.getElementText();
				out.modelParameters.put(elementName, value);
			}
			
			
			//
			// Element finish
			//
			
			if (eventType == XMLEvent.END_ELEMENT) {
				String elementName = reader.getName().getLocalPart();
				if (elementName.equals(XML_MODEL)) return;
			}
		}
	}
	
	
	/**
	 * Read edge specs from the XML
	 * 
	 * @param reader the reader
	 * @param file the input file
	 * @param out the specs
	 * @throws XMLStreamException on an XML stream error
	 * @throws IOException on I/O error
	 */
	private static void loadEdgeSpecsFromXML(XMLStreamReader reader, File file, FGFGraphGeneratorSpecs out)
			throws XMLStreamException, IOException {
		
		assert reader.getName().getLocalPart().equals(XML_EDGES);
		
		while (reader.hasNext()) {
			int eventType = reader.next();
		
			
			//
			// Element start
			//
			
			if (eventType == XMLEvent.START_ELEMENT) {
				String elementName = reader.getName().getLocalPart();
				
				if (elementName.equals(XML_EDGES_LABELS)) {
					loadEdgeLabelsFromXML(reader, file, out);
					continue;
				}
				
				if (elementName.equals(XML_PROPERTIES)) {
					loadPropertiesFromXML(reader, file, out.edgeProperties);
					continue;
				}

				throw new RuntimeException("Invalid XML element <" + elementName + ">.");
			}
			
			
			//
			// Element finish
			//
			
			if (eventType == XMLEvent.END_ELEMENT) {
				String elementName = reader.getName().getLocalPart();
				if (elementName.equals(XML_EDGES)) return;
			}
		}
	}
	
	
	/**
	 * Read edge labels from the XML
	 * 
	 * @param reader the reader
	 * @param file the input file
	 * @param out the specs
	 * @throws XMLStreamException on an XML stream error
	 */
	private static void loadEdgeLabelsFromXML(XMLStreamReader reader, File file, FGFGraphGeneratorSpecs out)
			throws XMLStreamException {
		
		assert reader.getName().getLocalPart().equals(XML_EDGES_LABELS);
		boolean edgeLabelsDistributionAlreadySpecified = false;
		
		while (reader.hasNext()) {
			int eventType = reader.next();
		
			
			//
			// Element start
			//
			
			if (eventType == XMLEvent.START_ELEMENT) {
				String elementName = reader.getName().getLocalPart();
				
				if (elementName.equals(XML_EDGES_LABELS_LABEL)) {
					String value = reader.getElementText();
					out.edgeLabels.add(value);
					continue;
				}
				
				if (elementName.equals(XML_DISTRIBUTION)) {
					if (edgeLabelsDistributionAlreadySpecified) {
						throw new RuntimeException("Cannot specify more than one distribution of edge labels.");
					}
					edgeLabelsDistributionAlreadySpecified = true;
					out.edgeLabelsDistribution = loadDistributionFromXML(reader);
					continue;
				}
				
				throw new RuntimeException("Invalid XML element <" + elementName + "> inside <" + XML_EDGES_LABELS + ">.");
			}
			
			
			//
			// Element finish
			//
			
			if (eventType == XMLEvent.END_ELEMENT) {
				String elementName = reader.getName().getLocalPart();
				if (elementName.equals(XML_EDGES_LABELS)) return;
			}
		}
	}
	
	
	/**
	 * Read vertex specs from the XML
	 * 
	 * @param reader the reader
	 * @param file the input file
	 * @param out the specs
	 * @throws XMLStreamException on an XML stream error
	 * @throws IOException on I/O error
	 */
	private static void loadVertexSpecsFromXML(XMLStreamReader reader, File file, FGFGraphGeneratorSpecs out)
			throws XMLStreamException, IOException {
		
		assert reader.getName().getLocalPart().equals(XML_VERTICES);
		
		while (reader.hasNext()) {
			int eventType = reader.next();
		
			
			//
			// Element start
			//
			
			if (eventType == XMLEvent.START_ELEMENT) {
				String elementName = reader.getName().getLocalPart();
				
				if (elementName.equals(XML_PROPERTIES)) {
					loadPropertiesFromXML(reader, file, out.vertexProperties);
					continue;
				}
				
				throw new RuntimeException("Invalid XML element <" + elementName + ">.");
			}
			
			
			//
			// Element finish
			//
			
			if (eventType == XMLEvent.END_ELEMENT) {
				String elementName = reader.getName().getLocalPart();
				if (elementName.equals(XML_VERTICES)) return;
			}
		}
	}
	
	
	/**
	 * Read edge or vertex properties from the XML
	 * 
	 * @param reader the reader
	 * @param file the input file
	 * @param out the list of properties
	 * @throws XMLStreamException on an XML stream error
	 * @throws IOException on I/O error
	 */
	private static void loadPropertiesFromXML(XMLStreamReader reader, File file, List<Property> out)
			throws XMLStreamException, IOException {
		
		assert reader.getName().getLocalPart().equals(XML_PROPERTIES);
		
		boolean inProperty = false;
		String p_name = null;
		short p_type = -1;
		Distribution p_distribution = null;
		HashMap<String, String> p_parameters = new HashMap<String, String>(); 
		
		while (reader.hasNext()) {
			int eventType = reader.next();
		
			
			//
			// Element start
			//
			
			if (eventType == XMLEvent.START_ELEMENT) {
				String elementName = reader.getName().getLocalPart();
				
				if (!inProperty) {					
					
					if (elementName.equals(XML_PROPERTIES_PROPERTY)) {
						p_name = reader.getAttributeValue(null, "name");
						String s_type = reader.getAttributeValue(null, "type");
						if (p_name == null || s_type == null) {
							throw new RuntimeException("A property must have a name and a type.");
						}
						p_type = FGFTypes.fromString(s_type);
						p_parameters.clear();
						p_distribution = null;
						inProperty = true;
						continue;
					}
				}
				else {
					
					if (elementName.equals(XML_DISTRIBUTION)) {
						if (p_distribution != null) {
							throw new RuntimeException("Cannot specify more than one distribution for a property.");
						}
						p_distribution = loadDistributionFromXML(reader);
					}
					else {
						String value = reader.getElementText();
						p_parameters.put(elementName, value);
					}
					
					continue;
				}
				
				throw new RuntimeException("Invalid XML element <" + elementName + "> inside <"
						+ (!inProperty ? XML_PROPERTIES : XML_PROPERTIES_PROPERTY) + ">.");
			}
			
			
			//
			// Element finish
			//
			
			if (eventType == XMLEvent.END_ELEMENT) {
				String elementName = reader.getName().getLocalPart();
				
				if (!inProperty) {
					if (elementName.equals(XML_PROPERTIES)) return;
				}
				else {
					if (elementName.equals(XML_PROPERTIES_PROPERTY)) {
						
						Property p = null;
						if (p_distribution == null) {
							p_distribution = new Distribution.Uniform();
						}
						
						String s_min, s_max, l;
						List<String> strings = null;
						
						switch (p_type) {
						case FGFTypes.STRING:
							String s_f = p_parameters.remove("file");
							if (s_f != null) {
								File f = s_f.startsWith("/") ? new File(s_f) : new File(file.getParentFile(), s_f);
								BufferedReader r = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(f))));
								strings = new ArrayList<String>();
								while ((l = r.readLine()) != null) strings.add(l);
								r.close();
							}
							if (!p_parameters.isEmpty()) {
								throw new RuntimeException("A string property tag has too many parameters.");
							}
							if (strings == null) {
								throw new RuntimeException("The list of strings is undefined.");
							}
							if (strings.isEmpty()) {
								throw new RuntimeException("The list of strings is empty.");
							}
							p = new Property.String(p_name, p_distribution, strings);
							break;
						case FGFTypes.BOOLEAN:
							if (!p_parameters.isEmpty()) {
								throw new RuntimeException("A boolean property tag has too many parameters.");
							}
							p = new Property.Boolean(p_name, p_distribution);
							break;
						case FGFTypes.SHORT:
							s_min = p_parameters.remove("min");
							s_max = p_parameters.remove("max");
							if (s_min == null || s_max == null) {
								throw new RuntimeException("A short property must have a min and a max.");
							}
							if (!p_parameters.isEmpty()) {
								throw new RuntimeException("A short property tag has too many parameters.");
							}
							p = new Property.Short(p_name, p_distribution, Short.parseShort(s_min), Short.parseShort(s_max));
							break;
						case FGFTypes.INTEGER:
							s_min = p_parameters.remove("min");
							s_max = p_parameters.remove("max");
							if (s_min == null || s_max == null) {
								throw new RuntimeException("An integer property must have a min and a max.");
							}
							if (!p_parameters.isEmpty()) {
								throw new RuntimeException("An integer property tag has too many parameters.");
							}
							p = new Property.Integer(p_name, p_distribution, Integer.parseInt(s_min), Integer.parseInt(s_max));
							break;
						case FGFTypes.LONG:
							s_min = p_parameters.remove("min");
							s_max = p_parameters.remove("max");
							if (s_min == null || s_max == null) {
								throw new RuntimeException("A long property must have a min and a max.");
							}
							if (!p_parameters.isEmpty()) {
								throw new RuntimeException("A long property tag has too many parameters.");
							}
							p = new Property.Long(p_name, p_distribution, Long.parseLong(s_min), Long.parseLong(s_max));
							break;
						case FGFTypes.FLOAT:
							s_min = p_parameters.remove("min");
							s_max = p_parameters.remove("max");
							if (s_min == null || s_max == null) {
								throw new RuntimeException("A float property must have a min and a max.");
							}
							if (!p_parameters.isEmpty()) {
								throw new RuntimeException("A float property tag has too many parameters.");
							}
							p = new Property.Float(p_name, p_distribution, Float.parseFloat(s_min), Float.parseFloat(s_max));
							break;
						case FGFTypes.DOUBLE:
							s_min = p_parameters.remove("min");
							s_max = p_parameters.remove("max");
							if (s_min == null || s_max == null) {
								throw new RuntimeException("A double property must have a min and a max.");
							}
							if (!p_parameters.isEmpty()) {
								throw new RuntimeException("A double property tag has too many parameters.");
							}
							p = new Property.Double(p_name, p_distribution, Double.parseDouble(s_min), Double.parseDouble(s_max));
							break;
						default:
							throw new RuntimeException("Unsupported property type: " + FGFTypes.toString(p_type));
						}
						
						out.add(p);
						inProperty = false;
					}
				}
			}
		}
	}
	
	
	/**
	 * Read a distribution from the XML
	 * 
	 * @param reader the reader
	 * @return the distribution
	 * @throws XMLStreamException on an XML stream error
	 */
	private static Distribution loadDistributionFromXML(XMLStreamReader reader) throws XMLStreamException {
		
		assert reader.getName().getLocalPart().equals(XML_DISTRIBUTION);
		
		String name = reader.getAttributeValue(null, "name");
		if (name == null) {
			throw new RuntimeException("The distribution name must be specified");
		}
		HashMap<String, String> parameters = new HashMap<String, String>(); 
		
		while (reader.hasNext()) {
			int eventType = reader.next();
		
			
			//
			// Element start
			//
			
			if (eventType == XMLEvent.START_ELEMENT) {
				String elementName = reader.getName().getLocalPart();
				String value = reader.getElementText();
				parameters.put(elementName, value);
			}
			
			
			//
			// Element finish
			//
			
			if (eventType == XMLEvent.END_ELEMENT) {
				String elementName = reader.getName().getLocalPart();
				
				if (elementName.equals(XML_DISTRIBUTION)) {
					
					if ("uniform".equalsIgnoreCase(name)) {
						if (!parameters.isEmpty()) {
							throw new RuntimeException("The uniform distribution does not take any parameters");
						}
						return new Distribution.Uniform();
					}
					else if ("normal".equalsIgnoreCase(name) || "gaussian".equalsIgnoreCase(name)) {
						if (!parameters.isEmpty()) {
							throw new RuntimeException("The normal distribution does not take any parameters");
						}
						return new Distribution.Uniform();
					}
					else {
						throw new RuntimeException("Unsupported probabilistic distribution: " + name);
					}
				}
			}
		}
		
		throw new InternalError();
	}
	
	
	/**
	 * Generate a number with a normal distribution, mean 0, and standard deviation 1
	 * 
	 * @return the random number
	 */
	private static double randomGaussian() {
		return random.get().nextGaussian();
	}
	
	
	/**
	 * Property specs
	 */
	public static abstract class Property {
		
		private java.lang.String name;
		private short type;
		private Distribution distribution;
		
		
		/**
		 * Create an instance of class Property
		 * 
		 * @param name the property name
		 * @param type the property type
			 * @param distribution the probabilistic distribution
		 */
		public Property(java.lang.String name, short type, Distribution distribution) {
			this.name = name;
			this.type = type;
			this.distribution = distribution;
		}
		
		
		/**
		 * Get the property name
		 * 
		 * @return the property name
		 */
		public java.lang.String getName() {
			return name;
		}
		
		
		/**
		 * Get the property type
		 * 
		 * @return the property type
		 */
		public short getType() {
			return type;
		}
		
		
		/**
		 * Get the distribution
		 * 
		 * @return the distribution
		 */
		public Distribution getDistribution() {
			return distribution;
		}
		
		
		/**
		 * Generate a value
		 * 
		 * @param r a random number between 0 and 1
		 * @return a generated value
		 */
		public abstract Object generateValue(double r);
		
		
		/**
		 * Generate a value
		 * 
		 * @return a generated value
		 */
		public Object generateValue() {
			return generateValue(distribution.randomValue());
		}
		
		
		/**
		 * A string property
		 */
		public static class String extends Property {
			
			private List<java.lang.String> strings;
			
				
			/**
			 * Create an instance of class Property.String
			 * 
			 * @param name the property name
			 * @param distribution the probabilistic distribution
			 * @param strings the strings to choose from
			 */
			public String(java.lang.String name, Distribution distribution, List<java.lang.String> strings) {
				super(name, FGFTypes.STRING, distribution);
				this.strings = strings;
				if (strings == null || strings.isEmpty()) {
					throw new IllegalArgumentException("The strings array cannot be null or empty");
				}
			}
			
			
			/**
			 * Generate a value
			 * 
			 * @param r a random number between 0 and 1
			 * @return a generated value
			 */
			@Override
			public Object generateValue(double r) {
				return strings.get((int) (r * strings.size()));
			}
		}
		
		
		/**
		 * A boolean property
		 */
		public static class Boolean extends Property {
			
				
			/**
			 * Create an instance of class Property.Boolean
			 * 
			 * @param name the property name
			 * @param distribution the probabilistic distribution
			 * @param min the minimum value
			 * @param max the maximum value
			 */
			public Boolean(java.lang.String name, Distribution distribution) {
				super(name, FGFTypes.BOOLEAN, distribution);
			}
			
			
			/**
			 * Generate a value
			 * 
			 * @param r a random number between 0 and 1
			 * @return a generated value
			 */
			@Override
			public Object generateValue(double r) {
				return r >= 0.5;
			}
		}
		
		
		/**
		 * A short property
		 */
		public static class Short extends Property {
			
			private short min;
			private short max;
			
				
			/**
			 * Create an instance of class Property.Short
			 * 
			 * @param name the property name
			 * @param distribution the probabilistic distribution
			 * @param min the minimum value
			 * @param max the maximum value
			 */
			public Short(java.lang.String name, Distribution distribution, short min, short max) {
				super(name, FGFTypes.SHORT, distribution);
				this.min = min;
				this.max = max;
			}
			
			
			/**
			 * Generate a value
			 * 
			 * @param r a random number between 0 and 1
			 * @return a generated value
			 */
			@Override
			public Object generateValue(double r) {
				return min + (short)(r * (max - min + 1));
			}
		}
		
		
		/**
		 * An integer property
		 */
		public static class Integer extends Property {
			
			private int min;
			private int max;
			
				
			/**
			 * Create an instance of class Property.Integer
			 * 
			 * @param name the property name
			 * @param distribution the probabilistic distribution
			 * @param min the minimum value
			 * @param max the maximum value
			 */
			public Integer(java.lang.String name, Distribution distribution, int min, int max) {
				super(name, FGFTypes.INTEGER, distribution);
				this.min = min;
				this.max = max;
			}
			
			
			/**
			 * Generate a value
			 * 
			 * @param r a random number between 0 and 1
			 * @return a generated value
			 */
			@Override
			public Object generateValue(double r) {
				return min + (int)(r * (max - min + 1));
			}
		}
		
		
		/**
		 * A long property
		 */
		public static class Long extends Property {
			
			private long min;
			private long max;
			
				
			/**
			 * Create an instance of class Property.Long
			 * 
			 * @param name the property name
			 * @param distribution the probabilistic distribution
			 * @param min the minimum value
			 * @param max the maximum value
			 */
			public Long(java.lang.String name, Distribution distribution, long min, long max) {
				super(name, FGFTypes.LONG, distribution);
				this.min = min;
				this.max = max;
			}
			
			
			/**
			 * Generate a value
			 * 
			 * @param r a random number between 0 and 1
			 * @return a generated value
			 */
			@Override
			public Object generateValue(double r) {
				return min + (long)(r * (max - min + 1));
			}
		}
		
		
		/**
		 * A float property
		 */
		public static class Float extends Property {
			
			private float min;
			private float max;
			
				
			/**
			 * Create an instance of class Property.Float
			 * 
			 * @param name the property name
			 * @param distribution the probabilistic distribution
			 * @param min the minimum value
			 * @param max the maximum value
			 */
			public Float(java.lang.String name, Distribution distribution, float min, float max) {
				super(name, FGFTypes.LONG, distribution);
				this.min = min;
				this.max = max;
			}
			
			
			/**
			 * Generate a value
			 * 
			 * @param r a random number between 0 and 1
			 * @return a generated value
			 */
			@Override
			public Object generateValue(double r) {
				return min + (float)(r * (max - min + 1));
			}
		}
		
		
		/**
		 * A double property
		 */
		public static class Double extends Property {
			
			private double min;
			private double max;
			
				
			/**
			 * Create an instance of class Property.Double
			 * 
			 * @param name the property name
			 * @param distribution the probabilistic distribution
			 * @param min the minimum value
			 * @param max the maximum value
			 */
			public Double(java.lang.String name, Distribution distribution, double min, double max) {
				super(name, FGFTypes.LONG, distribution);
				this.min = min;
				this.max = max;
			}
			
			
			/**
			 * Generate a value
			 * 
			 * @param r a random number between 0 and 1
			 * @return a generated value
			 */
			@Override
			public Object generateValue(double r) {
				return min + (double)(r * (max - min + 1));
			}
		}
	}
	
	
	/**
	 * Distribution specs
	 */
	public static abstract class Distribution {
		
		/**
		 * Create an instance of class Distribution
		 */
		public Distribution() {
			// Nothing to do
		}
		
		
		/**
		 * Generate a random value between 0 (inclusive) and 1 (exclusive)
		 * 
		 * @return the random value
		 */
		public abstract double randomValue();
		
		
		/**
		 * Generate a random integer between 0 and the specified upper bound (exclusive)
		 * 
		 * @param upper the upper bound (exclusive)
		 * @return the random value
		 */
		public int randomInt(int upper) {
			return (int)(randomValue() * upper);
		}
		
		
		/**
		 * Uniform distribution
		 */
		public static class Uniform extends Distribution {
			
			/**
			 * Create an instance of class Distribution.Uniform
			 */
			public Uniform() {
				// Nothing to do
			}
			
			
			/**
			 * Generate a random value between 0 (inclusive) and 1 (exclusive)
			 * 
			 * @return the random value
			 */
			@Override
			public double randomValue() {
				return Math.random();
			}
		}
		
		
		/**
		 * Truncated normal / Gaussian distribution
		 */
		public static class Normal extends Distribution {
			
			/**
			 * Create an instance of class Distribution.Normal
			 */
			public Normal() {
				// Nothing to do
			}
			
			
			/**
			 * Generate a random value between 0 (inclusive) and 1 (exclusive)
			 * 
			 * @return the random value
			 */
			@Override
			public double randomValue() {
				double r;
				do {
					r = 0.5 + 0.25 * randomGaussian();
				}
				while (r < 0 || r >= 1);
				return r;
			}
		}
	}
}
