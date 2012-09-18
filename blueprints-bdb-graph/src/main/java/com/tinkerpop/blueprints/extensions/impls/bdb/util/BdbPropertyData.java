package com.tinkerpop.blueprints.extensions.impls.bdb.util;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class BdbPropertyData {
	public String pkey = null;
	public Object value = null;
    
    public String toString() {
    	return "duppropertydata[" + this.pkey + ":" + this.value + "]";
    }
}
