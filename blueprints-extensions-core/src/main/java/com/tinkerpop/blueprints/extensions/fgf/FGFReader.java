package com.tinkerpop.blueprints.extensions.fgf;

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
	private ObjectType[] vertexTypes;
	private ObjectType[] edgeTypes;

	
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
		
		propertyTypes = new PropertyType[(int) din.readLong()];
		
		vertexTypes = new ObjectType[(int) din.readLong()];
		for (int i = 0; i < vertexTypes.length; i++) {
			vertexTypes[i] = new ObjectType(din.readLong());
		}
		
		edgeTypes = new ObjectType[(int) din.readLong()];
		for (int i = 0; i < edgeTypes.length; i++) {
			edgeTypes[i] = new ObjectType(din.readLong());
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
	protected void finalize() {
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
		
		for (ObjectType t : vertexTypes) {
			
			ObjectInputStream iin = new ObjectInputStream(din);
			
			iin.read(header);
			assertMagic(header, "NODE");			
			t.name = iin.readUTF();
			
			if (handler != null) handler.vertexTypeStart(t.name, t.count);
			
			
			// Read the vertices
			
			for (long id = 0; id < t.count; id++) {
				
				readProperties(iin, properties);
				
				if (handler != null) handler.vertex(id, t.name, properties);
			}
			
			if (handler != null) handler.vertexTypeEnd(t.name, t.count);
		}
		
		
		// Read the edge types
		
		long id = 0;
		for (ObjectType t : edgeTypes) {
			
			ObjectInputStream iin = new ObjectInputStream(din);
			
			iin.read(header);
			assertMagic(header, "EDGE");
			t.name = iin.readUTF();
			
			if (handler != null) handler.edgeTypeStart(t.name, t.count);
			
			
			// Read the edges
			
			for (int i = 0; i < t.count; i++) {
				
				long head = iin.readLong();
				long tail = iin.readLong();
				readProperties(iin, properties);
				
				if (handler != null) handler.edge(id++, head, tail, t.name, properties);
			}
			
			if (handler != null) handler.edgeTypeEnd(t.name, t.count);
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
	private class ObjectType {
		
		public String name;
		public long count;
		
		public ObjectType(long count) {
			this.name = null;
			this.count = count;
		}
	}
}
