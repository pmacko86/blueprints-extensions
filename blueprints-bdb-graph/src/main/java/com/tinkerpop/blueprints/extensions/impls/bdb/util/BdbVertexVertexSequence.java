package com.tinkerpop.blueprints.extensions.impls.bdb.util;

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


/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 * @author Peter Macko (http://www.eecs.harvard.edu/~pmacko)
 */
public class BdbVertexVertexSequence implements CloseableIterable<Vertex> {

    private BdbGraph graph;
    private Direction direction;
    private DatabaseEntry id = new DatabaseEntry();
    private BdbVertexVertexSequenceIterator iterator = null;

    public BdbVertexVertexSequence(final BdbGraph graph, final DatabaseEntry id, final Direction direction) {
        
        this.graph = graph;
        this.id = id;
        this.direction = direction;
    }

    public Iterator<Vertex> iterator() {
        return (iterator = new BdbVertexVertexSequenceIterator());
    }
	
    @Override
	public void close() {
    	if (iterator != null) iterator.close();
	}

    
    class BdbVertexVertexSequenceIterator implements Iterator<Vertex> {
	
	    private Cursor cursor;
	    private DatabaseEntry key = new DatabaseEntry();
	    private DatabaseEntry data = new DatabaseEntry();
	    private boolean useStored = false;
	    
	    public BdbVertexVertexSequenceIterator() {
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
		
		public Vertex next() {
			if (this.cursor == null)
				throw new NoSuchElementException();
		    if (this.useStored) {
		    	this.useStored = false;
		    	BdbEdgeData edata = BdbEdge.edgeDataBinding.entryToObject(data);
		    	return new BdbVertex(graph, edata.id);
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
		    	return new BdbVertex(graph, edata.id);
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
