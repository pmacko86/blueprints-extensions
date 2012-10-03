package com.tinkerpop.blueprints.extensions.fgf;


/**
 * Fast Graph Format: Constants
 *
 * @author Peter Macko (http://eecs.harvard.edu/~pmacko)
 */
public class FGFTypes {
	
	public static final short OTHER   = 0x00;
	public static final short STRING  = 0x01;
	
	public static final short BOOLEAN = 0x10;
	public static final short SHORT   = 0x11;
	public static final short INTEGER = 0x12;
	public static final short LONG    = 0x13;
	
	public static final short FLOAT   = 0x20;
	public static final short DOUBLE  = 0x21;
	
	
	/**
	 * Get the attribute type for the given value
	 * 
	 * @param value the value
	 * @return the attribute code
	 */
	public static short fromSampleValue(Object value) {
		
		if (value instanceof String ) return STRING;
		if (value instanceof Boolean) return BOOLEAN;
		
		if (value instanceof Short  ) return SHORT;
		if (value instanceof Integer) return INTEGER;
		if (value instanceof Long   ) return LONG;
		
		if (value instanceof Float  ) return FLOAT;
		if (value instanceof Double ) return DOUBLE;
		
		return OTHER;
	}
}
