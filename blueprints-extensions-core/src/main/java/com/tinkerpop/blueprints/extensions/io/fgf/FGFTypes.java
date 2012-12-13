package com.tinkerpop.blueprints.extensions.io.fgf;


/**
 * Fast Graph Format: Type Constants
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
	
	
	/**
	 * Convert type number to a string
	 * 
	 * @param type the type
	 * @return the string
	 */
	public static String toString(short type) {
		
		switch (type) {
		
		case OTHER  : return "other";
		case STRING : return "string";

		case BOOLEAN: return "boolean";
		case SHORT  : return "short";
		case INTEGER: return "integer";
		case LONG   : return "long";

		case FLOAT  : return "float";
		case DOUBLE : return "double";
		
		default:
			throw new IllegalArgumentException("Invalid FGF property type code");
		}
	}
	
	
	/**
	 * Convert a string type name to its numerical code
	 * 
	 * @param str the string name of the type
	 * @return its numerical value
	 */
	public static short fromString(String str) {
		
		if ("other"  .equalsIgnoreCase(str)) return OTHER;
		if ("string" .equalsIgnoreCase(str)) return STRING;
		if ("boolean".equalsIgnoreCase(str)) return BOOLEAN;
		if ("short"  .equalsIgnoreCase(str)) return SHORT;
		if ("integer".equalsIgnoreCase(str)) return INTEGER;
		if ("long"   .equalsIgnoreCase(str)) return LONG;
		if ("float"  .equalsIgnoreCase(str)) return FLOAT;
		if ("double" .equalsIgnoreCase(str)) return DOUBLE;
		
		throw new IllegalArgumentException("Invalid FGF property type name");
	}
}
