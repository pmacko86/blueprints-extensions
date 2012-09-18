package com.tinkerpop.blueprints.extensions.impls.bdb.util;

import com.sleepycat.bind.RecordNumberBinding;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseEntry; 
import com.sleepycat.db.OperationStatus; 
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.extensions.impls.bdb.BdbEdge;
import com.tinkerpop.blueprints.extensions.impls.bdb.BdbGraph;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 * @author Peter Macko (http://www.eecs.harvard.edu/~pmacko)
 */
public class BdbEdgeSequence implements CloseableIterable<Edge> {

    private BdbGraph graph;
    private BdbEdgeSequenceIterator iterator = null;
        
    public BdbEdgeSequence(final BdbGraph graph)
    {
    	this.graph = graph;
    }

    public Iterator<Edge> iterator() {
    	iterator = new BdbEdgeSequenceIterator();
        return iterator;
    }
	
    @Override
	public void close() {
    	if (iterator != null) iterator.close();
	}

    class BdbEdgeSequenceIterator implements Iterator<Edge> {
	    	
	    private Cursor cursor;
	    private DatabaseEntry key = new DatabaseEntry();
	    private DatabaseEntry data = new DatabaseEntry();
	    private boolean useStored = false;
	    
	    public BdbEdgeSequenceIterator()
	    {
	    	OperationStatus status;
	    	
	        try {
	            this.cursor = graph.outDb.openCursor(null, null);
	            status = this.cursor.getFirst(this.key, this.data, null);
	        } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	         
	        if (status == OperationStatus.SUCCESS)
	        	this.useStored = true;
	        else
	        	this.close();
	    }
		
		public Edge next() {
			if (this.cursor == null)
				throw new NoSuchElementException();
		    if (this.useStored) {
		    	this.useStored = false;
		    	BdbEdgeData edata = BdbEdge.edgeDataBinding.entryToObject(data);
		    	return new BdbEdge(graph, RecordNumberBinding.entryToRecordNumber(this.key), edata.label, edata.id);
		    }
		    
		    OperationStatus status;
		    
		    try {
	            status = this.cursor.getNext(this.key, this.data, null);   	 
		    } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	               
	        if (status == OperationStatus.SUCCESS) {
		    	BdbEdgeData edata = BdbEdge.edgeDataBinding.entryToObject(data);
		    	return new BdbEdge(graph, RecordNumberBinding.entryToRecordNumber(this.key), edata.label, edata.id);
	        } else {
	            this.close();
	            throw new NoSuchElementException();
	        }		
		}
	
		public boolean hasNext() {
			if (cursor == null)
				return false;
			if (this.useStored)
				return true;
			
		    OperationStatus status;
		    
		    try {
	            status = this.cursor.getNext(this.key, this.data, null);   	 
		    } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
		    
	        if (status == OperationStatus.SUCCESS) {
	        	this.useStored = true;
	        	return true;
	        } else {
	            this.close();
	            return false;
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
	        	this.key = null;
	        	this.cursor = null;
	        }
		}
	
	    public void remove() { 
	        throw new UnsupportedOperationException(); 
	    }
    }
}
