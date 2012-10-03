package com.tinkerpop.blueprints.extensions.io.fgf;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Fast Graph Format: Writer
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class FGFWriter implements Closeable {
	
	private File file;
	private FileOutputStream out;
	
	private File propertyTypeFile;
	private RandomAccessFile propertyTypeOut;
	
	private Map<String, ObjectType> vertexTypes = new HashMap<String, FGFWriter.ObjectType>(); 
	private Map<String, ObjectType> edgeTypes = new HashMap<String, FGFWriter.ObjectType>(); 
	
	private boolean closed = false;
	private Map<String, PropertyType> propertyTypes = new HashMap<String, PropertyType>();
	

	/**
	 * Create an instance of class FGFWriter and open the file for writing
	 * 
	 * @param file the file
	 * @throws IOException on error
	 */
	public FGFWriter(File file) throws IOException {
		
		this.file = file;
		out = new FileOutputStream(file);
		
		propertyTypeFile = File.createTempFile(file.getName(), ".tmp");
		propertyTypeFile.deleteOnExit();
		propertyTypeOut = new RandomAccessFile(propertyTypeFile, "rw");
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
		
		
		// Header
		
		out.write('F');
		out.write('G');
		out.write('F');
		out.write('1');
		
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.clear();
		bb.putLong(propertyTypes.size());
		out.write(bb.array());

		bb.rewind();
		bb.clear();
		bb.putLong(vertexTypes.size());
		out.write(bb.array());
		for (Entry<String, ObjectType> p : vertexTypes.entrySet()) {
			bb.rewind();
			bb.clear();
			bb.putLong(p.getValue().count);
			out.write(bb.array());
		}

		bb.rewind();
		bb.clear();
		bb.putLong(edgeTypes.size());
		out.write(bb.array());
		for (Entry<String, ObjectType> p : edgeTypes.entrySet()) {
			bb.rewind();
			bb.clear();
			bb.putLong(p.getValue().count);
			out.write(bb.array());
		}

		out.write('A');
		out.write('T');
		out.write('T');
		out.write('R');
		
		out.flush();
		
		
		// Data
		
		byte[] buffer = new byte[64 * 1024];
		int c;
		
		propertyTypeOut.seek(0);
		while ((c = propertyTypeOut.read(buffer)) > 0) {
			out.write(buffer, 0, c);
		}
		propertyTypeOut.close();
		propertyTypeFile.delete();
		
		for (Entry<String, ObjectType> p : vertexTypes.entrySet()) {
			p.getValue().out.close();
			FileInputStream fin = new FileInputStream(p.getValue().file);
			while ((c = fin.read(buffer)) > 0) {
				out.write(buffer, 0, c);
			}
			fin.close();
			p.getValue().file.delete();
		}
		
		for (Entry<String, ObjectType> p : edgeTypes.entrySet()) {
			p.getValue().out.close();
			FileInputStream fin = new FileInputStream(p.getValue().file);
			while ((c = fin.read(buffer)) > 0) {
				out.write(buffer, 0, c);
			}
			fin.close();
			p.getValue().file.delete();
		}

		out.write('E');
		out.write('N');
		out.write('D');
		out.write('G');

		out.close();
		
		closed = true;
	}
	
	
	/**
	 * Get a property type
	 * 
	 * @param key the key
	 * @param sampleValue a sample value
	 * @return the type
	 * @throws IOException on error
	 */
	private PropertyType getPropertyType(String key, Object sampleValue) throws IOException {
		PropertyType t = propertyTypes.get(key);
		if (t == null) {
			t = new PropertyType(FGFTypes.fromSampleValue(sampleValue));
			propertyTypeOut.writeUTF(key);
			propertyTypeOut.writeShort(t.type);
			propertyTypes.put(key, t);
		}
		return t;
	}
	
	
	/**
	 * Get a vertex type
	 * 
	 * @param key the key
	 * @return the type
	 * @throws IOException on error
	 */
	private ObjectType getVertexType(String key) throws IOException {
		ObjectType t = vertexTypes.get(key);
		if (t == null) {
			t = new ObjectType(true, key);
			vertexTypes.put(key, t);
		}
		return t;
	}
	
	
	/**
	 * Get a vertex type
	 * 
	 * @param key the key
	 * @return the type
	 * @throws IOException on error
	 */
	private ObjectType getEdgeType(String key) throws IOException {
		ObjectType t = edgeTypes.get(key);
		if (t == null) {
			t = new ObjectType(false, key);
			edgeTypes.put(key, t);
		}
		return t;
	}
	
	
	/**
	 * Write the properties
	 * 
	 * @param out the output stream
	 * @param properties the map of properties (can be null)
	 * @throws IOException on I/O error
	 */
	private void writeProperties(ObjectOutputStream out, Map<String, Object> properties) throws IOException {
		
		if (properties != null) {
			
			if (properties.size() >= Byte.MAX_VALUE) {
				out.write(Byte.MAX_VALUE);
				out.writeLong(properties.size());
			}
			else {
				out.write(properties.size());
			}
			
			for (Entry<String, Object> p : properties.entrySet()) {
				Object value = p.getValue();
				PropertyType t = getPropertyType(p.getKey(), value);
				
				if (t.index >= Byte.MAX_VALUE) {
					out.write(Byte.MAX_VALUE);
					out.writeLong(t.index);
				}
				else {
					out.write(t.index);
				}
				
				switch (t.type) {
				case FGFTypes.BOOLEAN: out.writeBoolean((Boolean) value); break;
				case FGFTypes.STRING : out.writeUTF    ((String ) value); break;
				case FGFTypes.SHORT  : out.writeShort  ((Short  ) value); break;
				case FGFTypes.INTEGER: out.writeInt    ((Integer) value); break;
				case FGFTypes.LONG   : out.writeLong   ((Long   ) value); break;
				case FGFTypes.DOUBLE : out.writeDouble ((Double ) value); break;
				case FGFTypes.FLOAT  : out.writeFloat  ((Float  ) value); break;
				default:
					out.writeObject(value);
				}
			}
		}
		else {
			out.write(0);
		}
	}
	
	
	/**
	 * Write a vertex
	 * 
	 * @param type the vertex type
	 * @param properties the vertex properties (can be null)
	 * @return the vertex ID
	 * @throws IOException on error
	 */
	public long writeVertex(String type, Map<String, Object> properties) throws IOException {
		
		ObjectType t = getVertexType(type);
		long id = t.count++;
				
		writeProperties(t.out, properties);
		
		return id;
	}
	
	
	/**
	 * Write an edge
	 * 
	 * @param head the head vertex id
	 * @param tail the tail vertex id
	 * @param type the edge type (label)
	 * @param properties the vertex properties (can be null)
	 * @throws IOException on error
	 */
	public void writeEdge(long head, long tail, String type, Map<String, Object> properties) throws IOException {
		
		ObjectType t = getEdgeType(type);
		t.count++;
		
		t.out.writeLong(head);
		t.out.writeLong(tail);
		writeProperties(t.out, properties);
	}

	
	/**
	 * Property type
	 */
	private class PropertyType {
		
		public int index;
		public int type;
		
		public PropertyType(int type) {
			this.index = propertyTypes.size();
			this.type = type;
		}
	}
	
	
	/**
	 * Object type
	 */
	private class ObjectType {
		
		public File file;
		public ObjectOutputStream out;
		public int count = 0;
		
		public ObjectType(boolean vertex, String name) throws IOException {
			
			file = File.createTempFile(FGFWriter.this.file.getName(), ".tmp");
			file.deleteOnExit();
			out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
			
			if (vertex) {
				out.writeByte('N');
				out.writeByte('O');
				out.writeByte('D');
				out.writeByte('E');
			}
			else {
				out.writeByte('E');
				out.writeByte('D');
				out.writeByte('G');
				out.writeByte('E');
			}
			out.writeUTF(name);
		}
	}
}
