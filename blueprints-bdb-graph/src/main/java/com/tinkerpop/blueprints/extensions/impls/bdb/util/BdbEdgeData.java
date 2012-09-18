package com.tinkerpop.blueprints.extensions.impls.bdb.util;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class BdbEdgeData {
	public String label;
	public long id;
	
	public BdbEdgeData(final String label, final long id) {
		this.label = label;
		this.id = id;
	}

    public String toString() {
    	return "dupedgedata[" + this.label + ":" + this.id + "]";
    }
}
