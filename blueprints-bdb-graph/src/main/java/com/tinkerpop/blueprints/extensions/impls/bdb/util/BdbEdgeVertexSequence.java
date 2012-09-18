package com.tinkerpop.blueprints.extensions.impls.bdb.util;

import com.sleepycat.bind.RecordNumberBinding;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseEntry; 
import com.sleepycat.db.OperationStatus; 
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.extensions.impls.bdb.BdbEdge;
import com.tinkerpop.blueprints.extensions.impls.bdb.BdbGraph;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 * @author Peter Macko (http://www.eecs.harvard.edu/~pmacko)
 */
public class BdbEdgeVertexSequence implements CloseableIterable<Edge> {

    private BdbGraph graph;
    private Direction direction;
    private DatabaseEntry id = new DatabaseEntry();
    private BdbEdgeVertexSequenceIterator iterator = null;

    public BdbEdgeVertexSequence(final BdbGraph graph, final DatabaseEntry id, final Direction direction) {
        
        this.graph = graph;
        this.id = id;
        this.direction = direction;
    }

    public Iterator<Edge> iterator() {
        return (iterator = new BdbEdgeVertexSequenceIterator());
    }
	
    @Override
	public void close() {
    	if (iterator != null) iterator.close();
	}

    
    class BdbEdgeVertexSequenceIterator implements Iterator<Edge> {
	
	    private Cursor cursor;
	    private DatabaseEntry key = new DatabaseEntry();
	    private DatabaseEntry data = new DatabaseEntry();
	    private boolean useStored = false;
	    
	    public BdbEdgeVertexSequenceIterator() {
	    	OperationStatus status;
	    	
	        try {
	        	
	        	switch (direction) {
	        	case OUT: this.cursor = graph.outDb.openCursor(null, null); break;
	        	case IN : this.cursor = graph.inDb.openCursor(null, null); break;
	        	default : throw new IllegalArgumentException("Invalid direction");
	        	}
	        	
	            status = this.cursor.getSearchKey(id, this.data, null);
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
		    	switch (direction) {
	        	case OUT: return new BdbEdge(graph, RecordNumberBinding.entryToRecordNumber(id), edata.label, edata.id);
	        	case IN : return new BdbEdge(graph, edata.id, edata.label, RecordNumberBinding.entryToRecordNumber(id));
		    	}
		    }
		    
		    OperationStatus status;
		    
		    try {
		    	this.key.setPartial(0, 0, true);
	            status = this.cursor.getNextDup(id, this.data, null);
	            this.key.setPartial(false);
		    } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	               
	        if (status == OperationStatus.SUCCESS) {
		    	BdbEdgeData edata = BdbEdge.edgeDataBinding.entryToObject(data);
		    	switch (direction) {
	        	case OUT: return new BdbEdge(graph, RecordNumberBinding.entryToRecordNumber(id), edata.label, edata.id);
	        	case IN : return new BdbEdge(graph, edata.id, edata.label, RecordNumberBinding.entryToRecordNumber(id));
		    	}
		    } else {
	            this.close();
	            throw new NoSuchElementException();
	        }
	        
	        throw new IllegalStateException("Invalid direction");
		}
	
		public boolean hasNext() {
			if (cursor == null)
				return false;
			if (this.useStored)
				return true;
			
		    OperationStatus status;
		    
		    try {
		    	this.key.setPartial(0, 0, true);
	            status = this.cursor.getNextDup(this.key, this.data, null);
	            this.key.setPartial(false);
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
	        	this.cursor = null;
	         }
		}
	
	    public void remove() { 
	        throw new UnsupportedOperationException(); 
	    }
    }
}
