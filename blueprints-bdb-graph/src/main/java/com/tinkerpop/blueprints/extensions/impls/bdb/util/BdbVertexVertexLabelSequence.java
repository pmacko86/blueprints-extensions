package com.tinkerpop.blueprints.extensions.impls.bdb.util;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseEntry; 
import com.sleepycat.db.OperationStatus; 
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.impls.bdb.BdbEdge;
import com.tinkerpop.blueprints.extensions.impls.bdb.BdbGraph;
import com.tinkerpop.blueprints.extensions.impls.bdb.BdbVertex;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 * @author Peter Macko (http://www.eecs.harvard.edu/~pmacko)
 */
public class BdbVertexVertexLabelSequence implements CloseableIterable<Vertex> {

    private BdbGraph graph;
    private DatabaseEntry id;
    private SortedSet<String> labels = new TreeSet<String>();
    private Direction direction;
    private BdbVertexVertexLabelSequenceIterator iterator = null;
    
    public BdbVertexVertexLabelSequence(final BdbGraph graph, final DatabaseEntry id, final Direction direction, String[] labels) {
    	
    	for (String label : labels)
    		this.labels.add(label);
        
    	this.graph = graph;
        this.id = id;
        this.direction = direction;
    }

    public Iterator<Vertex> iterator() {
        return (iterator = new BdbVertexVertexLabelSequenceIterator());
    }
	
    @Override
	public void close() {
    	if (iterator != null) iterator.close();
	}

    
    class BdbVertexVertexLabelSequenceIterator implements Iterator<Vertex> {
		
	    private Cursor cursor;
	    private DatabaseEntry key = new DatabaseEntry();
	    private DatabaseEntry data = new DatabaseEntry();
	    private boolean useStored = false;
	    
	    public BdbVertexVertexLabelSequenceIterator() {
	    	
	    	StringBinding.stringToEntry(labels.first(), this.data);
	    	
	    	OperationStatus status;
	    	
	        try {
	        	switch (direction) {
	        	case OUT: this.cursor = graph.outDb.openCursor(null, null); break;
	        	case IN : this.cursor = graph.inDb.openCursor(null, null); break;
	        	default : throw new IllegalArgumentException("Invalid direction");
	        	}
	        	
	            status = this.cursor.getSearchBothRange(id, this.data, null);
	            if (status == OperationStatus.SUCCESS)
	            	status = this.cursor.getCurrent(id, this.data, null);
	        } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	        
	        if (status == OperationStatus.SUCCESS) {
	        	BdbEdgeData edata = BdbEdge.edgeDataBinding.entryToObject(this.data);
	        	if (labels.contains(edata.label))
	        		this.useStored = true;
	        	else
	        		this.close();
	        } else
	        	this.close();
	    }
		
		public Vertex next() {
			if (this.cursor == null)
				throw new NoSuchElementException();
		    if (this.useStored) {
		    	this.useStored = false;
		    	BdbEdgeData edata = BdbEdge.edgeDataBinding.entryToObject(data);
		    	return new BdbVertex(graph, edata.id);
		    }
		    
		    OperationStatus status;
		    
		    //XXX dmargo: This loop is obviously hackish to support the new Blueprints API.
		    // The correct implementation should search for labels in order.
		    this.key.setPartial(0, 0, true);
		    while (true) {
			    try {
		            status = this.cursor.getNextDup(this.key, this.data, null);   	 
			    } catch (RuntimeException e) {
		            throw e;
		        } catch (Exception e) {
		            throw new RuntimeException(e.getMessage(), e);
		        }
		               
		        if (status == OperationStatus.SUCCESS) {
			    	BdbEdgeData edata = BdbEdge.edgeDataBinding.entryToObject(data);
			    	if (labels.contains(edata.label)) {
			    		this.key.setPartial(false);
			    		return new BdbVertex(graph, edata.id);
			    	}
		    	} else {
		    		this.key.setPartial(false);
			        this.close();
			        throw new NoSuchElementException();
		    	}
		    }
		}
	
		public boolean hasNext() {
			if (cursor == null)
				return false;
			if (this.useStored)
				return true;
			
		    OperationStatus status;
		    
		    this.key.setPartial(0, 0, true);
		    while (true) {
			    try {
		            status = this.cursor.getNextDup(this.key, this.data, null);   	 
			    } catch (RuntimeException e) {
		            throw e;
		        } catch (Exception e) {
		            throw new RuntimeException(e.getMessage(), e);
		        }
			    
		        if (status == OperationStatus.SUCCESS) {
		        	BdbEdgeData edata = BdbEdge.edgeDataBinding.entryToObject(this.data);
		        	if (labels.contains(edata.label)) {
		        		this.key.setPartial(false);
		        		this.useStored = true;
		        		return true;
		        	}
		        } else {
		        	this.key.setPartial(false);
			        this.close();
			        return false;
		        }
		    }
		}
		
		public void close() {
		    try {
		    	if (this.cursor != null) this.cursor.close();
		    } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        } finally {
	        	this.useStored = false;
	        	this.data = null;
	        	this.cursor = null;
	        }
		}
	
	    public void remove() { 
	        throw new UnsupportedOperationException(); 
	    } 
    }
}
