package com.tinkerpop.blueprints.extensions.impls.bdb.util;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseEntry; 
import com.sleepycat.db.OperationStatus; 
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.impls.bdb.BdbGraph;
import com.tinkerpop.blueprints.extensions.impls.bdb.BdbVertex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 * @author Peter Macko (http://www.eecs.harvard.edu/~pmacko)
 */
public class BdbVertexSequence implements CloseableIterable<Vertex> {

    private BdbGraph graph;
    private BdbVertexSequenceIterator iterator = null;
    
    public BdbVertexSequence(final BdbGraph graph) {
    	this.graph = graph;
    }

    public Iterator<Vertex> iterator() {
    	iterator = new BdbVertexSequenceIterator();
        return iterator;
    }
	
    @Override
	public void close() {
    	if (iterator != null) iterator.close();
	}

    class BdbVertexSequenceIterator implements Iterator<Vertex> {
    	
        private Cursor cursor;
        private DatabaseEntry key = new DatabaseEntry();
        private DatabaseEntry data = new DatabaseEntry();
        private boolean useStoredKey = false;
	  
	    public BdbVertexSequenceIterator() {
	    	OperationStatus status;
	    	
	        try {
	            this.cursor = graph.vertexDb.openCursor(null, null);
	            status = this.cursor.getFirst(this.key, this.data, null);
	        } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	        
	        if (status == OperationStatus.SUCCESS)
	        	this.useStoredKey = true;
	        else
	        	this.close();
	    }
		
		public Vertex next() {
			if (this.cursor == null)
				throw new NoSuchElementException();
		    if (this.useStoredKey) {
		    	this.useStoredKey = false;
		    	return new BdbVertex(graph, this.key);
		    }
	    
		    OperationStatus status;  
		    try {
		        status = this.cursor.getNext(this.key, this.data, null);   	        
		    } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	        
	        if (status == OperationStatus.SUCCESS)
	        	return new BdbVertex(graph, this.key);
	        else {
	            this.close();
	            throw new NoSuchElementException();
	        }
		}
	
		public boolean hasNext() {
			if (this.cursor == null)
				return false;
			if (this.useStoredKey)
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
	        	this.useStoredKey = true;
	        	return true;
	        } else {
	            this.close();
	            return false;
	        }
		}
		
		public void close() {
		    try{
		    	if (this.cursor != null) this.cursor.close();
		    } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        } finally {
	        	this.useStoredKey = false;
	        	this.key = null;
	        	this.cursor = null;
	        }
		}
	
	    public void remove() { 
	        throw new UnsupportedOperationException(); 
	    } 
    }
}
