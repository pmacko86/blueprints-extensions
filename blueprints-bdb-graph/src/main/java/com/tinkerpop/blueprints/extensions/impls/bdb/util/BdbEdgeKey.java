package com.tinkerpop.blueprints.extensions.impls.bdb.util;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class BdbEdgeKey {
	public long out;
	public String label;
	public long in;
	
	public BdbEdgeKey(final long out, final String label, final long in) {
		this.out = out;
		this.label = label;
		this.in = in;
	}
    
    public String toString() {
    	return "dupedgekey[" + this.out + ":" + this.label + ":" + this.in + "]";
    }
}
