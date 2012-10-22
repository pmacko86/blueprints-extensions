package com.tinkerpop.blueprints.extensions.io.fgf;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;


/**
 * Fast Graph Format: Reader
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class FGFReader implements Closeable {
	
	private File file;
	private BufferedInputStream in;
	private DataInputStream din;
	private boolean closed = false;
	
	private PropertyType[] propertyTypes;
	private VertexType[] vertexTypes;
	private EdgeType[] edgeTypes;
	
	private long totalVertices;
	private long totalEdges;
	
	private long initialVertexId;
	private long initialEdgeId;

	
	/**
	 * Create an instance of class FGFWriter and open the file for writing
	 * 
	 * @param file the file
	 * @throws IOException on I/O or parse error 
	 */
	public FGFReader(File file) throws IOException {
		
		this.file = file;
		this.in = new BufferedInputStream(new FileInputStream(this.file));
		
		din = new DataInputStream(in);
		
		
		// Read the header
		
		byte[] header = new byte[4];
		
		din.read(header);
		assertMagic(header, "FGF1");
		
		
		// Read the metadata
		
		long metadataHeaderBytes = din.readLong();
		if (metadataHeaderBytes % 8 != 0) {
			throw new IOException("Invalid number of metadata header bytes -- must be a multiple of 8");
		}
		int fields = (int) metadataHeaderBytes / 8;
		
		initialVertexId = (fields--) > 0 ? din.readLong() : 0;
		initialEdgeId   = (fields--) > 0 ? din.readLong() : 0;
		
		while (fields > 0) {
			din.readLong();
			fields--;
		}
		
		
		// Read the object counts
		
		din.read(header);
		assertMagic(header, "CNTS");
		
		propertyTypes = new PropertyType[(int) din.readLong()];
		
		int objectTypeIndex = 0;
		totalVertices = 0;
		vertexTypes = new VertexType[(int) din.readLong()];
		for (int i = 0; i < vertexTypes.length; i++) {
			String name = din.readUTF();
			vertexTypes[i] = new VertexType(objectTypeIndex++, name, din.readLong());
			totalVertices += vertexTypes[i].size();
		}
		
		totalEdges = 0;
		edgeTypes = new EdgeType[(int) din.readLong()];
		for (int i = 0; i < edgeTypes.length; i++) {
			String name = din.readUTF();
			edgeTypes[i] = new EdgeType(objectTypeIndex++, name, din.readLong());
			totalEdges += edgeTypes[i].size();
		}
		
		
		// Read the property types
		
		din.read(header);
		assertMagic(header, "ATTR");
		
		for (int i = 0; i < propertyTypes.length; i++) {
			String name = din.readUTF();
			int type = din.readShort();
			propertyTypes[i] = new PropertyType(i, name, type);
		}
	}
	
	
	/**
	 * Destructor
	 */
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		try {
			close();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	
	/**
	 * Finalize and close the file
	 * 
	 * @throws IOException on error
	 */
	public void close() throws IOException {
		
		if (closed) return;
		in.close();
		closed = true;
	}
	
	
	/**
	 * Get the initial vertex ID
	 * 
	 * @return the initial vertex ID
	 */
	public long getInitialVertexId() {
		return initialVertexId;
	}
	
	
	/**
	 * Get the initial edge ID
	 * 
	 * @return the initial edge ID
	 */
	public long getInitialEdgeId() {
		return initialEdgeId;
	}
	
	
	/**
	 * Get the total number of vertices
	 * 
	 * @return the total number of vertices
	 */
	public long getNumberOfVertices() {
		return totalVertices;
	}
	
	
	/**
	 * Get the total number of edges
	 * 
	 * @return the total number of edges
	 */
	public long getNumberOfEdges() {
		return totalEdges;
	}
	
	
	/**
	 * Get all property types
	 * 
	 * @return an array of all property types
	 */
	public PropertyType[] getPropertyTypes() {
		return propertyTypes;
	}
	
	
	/**
	 * Get all vertex types
	 * 
	 * @return an array of all vertex types
	 */
	public VertexType[] getVertexTypes() {
		return vertexTypes;
	}
	
	
	/**
	 * Get all edge types
	 * 
	 * @return an array of all edge types
	 */
	public EdgeType[] getEdgeTypes() {
		return edgeTypes;
	}
	
	
	/**
	 * Read properties
	 * 
	 * @param oin the object input stream
	 * @param out the output property map
	 * @throws IOException on I/O or parse error 
	 * @throws ClassNotFoundException if a property cannot be loaded due to a missing class
	 */
	private void readProperties(ObjectInputStream oin, Map<PropertyType, Object> out) throws IOException, ClassNotFoundException {
		
		out.clear();
		
		long np = oin.read();
		if (np == Byte.MAX_VALUE) np = oin.readLong();
		
		for (long i = 0; i < np; i++) {
			
			long nt = oin.read();
			if (nt == Byte.MAX_VALUE) nt = oin.readLong();
			PropertyType t = propertyTypes[(int) nt];
			
			Object value = null;
			switch (t.type) {
			case FGFTypes.BOOLEAN: value = oin.readBoolean(); break;
			case FGFTypes.STRING : value = oin.readUTF    (); break;
			case FGFTypes.SHORT  : value = oin.readShort  (); break;
			case FGFTypes.INTEGER: value = oin.readInt    (); break;
			case FGFTypes.LONG   : value = oin.readLong   (); break;
			case FGFTypes.DOUBLE : value = oin.readDouble (); break;
			case FGFTypes.FLOAT  : value = oin.readFloat  (); break;
			default:
				value = oin.readObject();
			}

			out.put(t, value);
		}
	}
	
	
	/**
	 * Read the file
	 * 
	 * @param handler the reader handler
	 * @throws IOException on I/O or parse error 
	 * @throws ClassNotFoundException if a property cannot be loaded due to a missing class
	 */
	public void read(FGFReaderHandler handler) throws IOException, ClassNotFoundException {
		
		byte[] header = new byte[4];
		Map<PropertyType, Object> properties = new HashMap<PropertyType, Object>();
		
		
		// Call the handler for all properties
		
		if (handler != null) {
			for (PropertyType t : propertyTypes) handler.propertyType(t);
		}
		
		
		// Read the vertex types
		
		long id = initialVertexId;
		for (VertexType t : vertexTypes) {
			
			ObjectInputStream iin = new ObjectInputStream(din);
			
			iin.read(header);
			assertMagic(header, "NODE");
			String name = iin.readUTF();
			if (!t.getName().equals(name)) {
				throw new IOException("Vertex type name mismatch: " + t.getName() + " expected, but " + name + " found");
			}
			
			if (handler != null) handler.vertexTypeStart(t, t.size());
			
			
			// Read the vertices
			
			for (long i = 0; i < t.size(); i++) {
				
				readProperties(iin, properties);
				
				if (handler != null) handler.vertex(id++, t, properties);
			}
			
			if (handler != null) handler.vertexTypeEnd(t, t.size());
		}
		
		
		// Read the edge types
		
		id = initialEdgeId;
		for (EdgeType t : edgeTypes) {
			
			ObjectInputStream iin = new ObjectInputStream(din);
			
			iin.read(header);
			assertMagic(header, "EDGE");
			String name = iin.readUTF();
			if (!t.getName().equals(name)) {
				throw new IOException("Edge type name mismatch: " + t.getName() + " expected, but " + name + " found");
			}
			
			if (handler != null) handler.edgeTypeStart(t, t.size());
			
			
			// Read the edges
			
			for (int i = 0; i < t.size(); i++) {
				
				long head = iin.readLong();
				long tail = iin.readLong();
				readProperties(iin, properties);
				
				if (handler != null) handler.edge(id++, head, tail, t, properties);
			}
			
			if (handler != null) handler.edgeTypeEnd(t, t.size());
		}
		
		
		// Finish
		
		din.read(header);
		assertMagic(header, "ENDG");
		
		close();
	}
	
	
	/**
	 * Assert that the magic value holds
	 * 
	 * @param buffer the byte buffer
	 * @param magic the magic value
	 * @throws IOException if the assertion does not hold
	 */
	private void assertMagic(byte[] buffer, String magic) throws IOException {
		for (int i = 0; i < magic.length(); i++) {
			if (buffer[i] != magic.charAt(i)) {
				throw new IOException("Invalid FGF magic value, expected \"" + magic + "\"");
			}
		}
	}
	
	
	/**
	 * Property type
	 */
	public class PropertyType {
		
		private int index;
		private String name;
		private int type;
		private Object aux;
		
		
		/**
		 * Create an instance of PropertyType
		 * 
		 * @param name the property name
		 * @param type the property type
		 */
		private PropertyType(int index, String name, int type) {
			this.index = index;
			this.name = name;
			this.type = type;
			this.aux = null;
		}
		
		
		/**
		 * Get the property index
		 * 
		 * @return the property index
		 */
		public int getIndex() {
			return index;
		}
		
		
		/**
		 * Get the property name
		 * 
		 * @return the property name
		 */
		public String getName() {
			return name;
		}
		
		
		/**
		 * Get the property type
		 * 
		 * @return the property type code
		 */
		public int getType() {
			return type;
		}
		
		
		/**
		 * Get the user-supplied auxiliary information
		 * 
		 * @return the auxiliary object
		 */
		public Object getAux() {
			return aux;
		}
		
		
		/**
		 * Set the user-supplied auxiliary information
		 * 
		 * @param aux the auxiliary object
		 */
		public void setAux(Object aux) {
			this.aux = aux;
		}
		
		
		/**
		 * Check whether the property type is equal to another property type
		 * 
		 * @param other the other object
		 * @return true if they are equal
		 */
		@Override
		public boolean equals(Object other) {
			if (!(other instanceof PropertyType)) return false;
			return ((PropertyType) other).index == index;
		}
		
		
		/**
		 * Compute the hash code
		 * 
		 * @return the hash code
		 */
		@Override
		public int hashCode() {
			return index;
		}
	}
	
	
	/**
	 * Object type
	 */
	public abstract class ObjectType {
		
		private int index;
		private String name;
		private long count;
		private Object aux;
		
		
		private ObjectType(int index, String name, long count) {
			this.index = index;
			this.name = name;
			this.count = count;
			this.aux = null;
		}

		
		/**
		 * Return the number of object of the given type
		 * 
		 * @return the number of the objects
		 */
		public long size() {
			return count;
		}
		
		
		/**
		 * Get the type name
		 * 
		 * @return the vertex or edge type name
		 */
		public String getName() {
			return name;
		}
		
		
		/**
		 * Get the user-supplied auxiliary information
		 * 
		 * @return the auxiliary object
		 */
		public Object getAux() {
			return aux;
		}
		
		
		/**
		 * Set the user-supplied auxiliary information
		 * 
		 * @param aux the auxiliary object
		 */
		public void setAux(Object aux) {
			this.aux = aux;
		}
		
		
		/**
		 * Check whether the property type is equal to another property type
		 * 
		 * @param other the other object
		 * @return true if they are equal
		 */
		@Override
		public boolean equals(Object other) {
			if (!(other instanceof PropertyType)) return false;
			return ((PropertyType) other).index == index;
		}
		
		
		/**
		 * Compute the hash code
		 * 
		 * @return the hash code
		 */
		@Override
		public int hashCode() {
			return index;
		}
	}
	
	
	/**
	 * A vertex type
	 */
	public class VertexType extends ObjectType {
		
		private VertexType(int index, String name, long count) {
			super(index, name, count);
		}
	}
	
	
	/**
	 * An edge type
	 */
	public class EdgeType extends ObjectType {
		
		private EdgeType(int index, String name, long count) {
			super(index, name, count);
		}
	}
}
