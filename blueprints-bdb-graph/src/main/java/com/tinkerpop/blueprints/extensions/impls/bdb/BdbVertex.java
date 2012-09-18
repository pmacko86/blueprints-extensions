package com.tinkerpop.blueprints.extensions.impls.bdb;

import com.sleepycat.bind.RecordNumberBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.OperationStatus;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.impls.bdb.util.BdbEdgeVertexLabelSequence;
import com.tinkerpop.blueprints.extensions.impls.bdb.util.BdbEdgeVertexSequence;
import com.tinkerpop.blueprints.extensions.impls.bdb.util.BdbPropertyData;
import com.tinkerpop.blueprints.extensions.impls.bdb.util.BdbVertexVertexLabelSequence;
import com.tinkerpop.blueprints.extensions.impls.bdb.util.BdbVertexVertexSequence;
import com.tinkerpop.blueprints.util.MultiIterable;
import com.tinkerpop.blueprints.util.StringFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 * @author Peter Macko (http://www.eecs.harvard.edu/~pmacko)
 */
public class BdbVertex extends BdbElement implements Vertex {
		
    private BdbGraph graph;
    protected DatabaseEntry id = new DatabaseEntry();

    protected BdbVertex(final BdbGraph graph) throws DatabaseException{
    	DatabaseEntry data = graph.data;
		data.setSize(0);
		if (graph.vertexDb.append(null, this.id, data) != OperationStatus.SUCCESS)
			throw new RuntimeException("BdbVertex: Failed to create vertex ID.");
			
		this.graph = graph;
    }

    protected BdbVertex(final BdbGraph graph, Object id) throws DatabaseException {
    	
    	if (id instanceof String)
    		id = Long.valueOf((String) id);

    	if(!(id instanceof Long))
    		throw new IllegalArgumentException("BdbVertex: " + id + " is not a valid vertex ID.");
    	
    	RecordNumberBinding.recordNumberToEntry((Long) id, this.id);
		if (graph.vertexDb.exists(null, this.id) != OperationStatus.SUCCESS)
			throw new RuntimeException("BdbVertex: Vertex " + id + " does not exist.");

        this.graph = graph;
    }

    public BdbVertex(final BdbGraph graph, final long id) {
    	RecordNumberBinding.recordNumberToEntry(id, this.id);
        this.graph = graph;
    }

    public BdbVertex(final BdbGraph graph, final DatabaseEntry id) {
    	this.graph = graph;
    	this.id.setData(id.getData().clone());
    }  
    
    public static BdbVertex getRandomVertex(final BdbGraph graph) throws DatabaseException {
    	
    	DatabaseEntry key = graph.key;
    	DatabaseEntry data = graph.data;
    	
    	// Note: This implementation assumes that the number of vertex deletions is negligible
    	// as compared to the total number of nodes
    	
		// Get the last ID# in the graph.
		Cursor cursor = graph.vertexDb.openCursor(null, null);
        OperationStatus status = cursor.getLast(key, data, null);
        cursor.close();
        if (status == OperationStatus.NOTFOUND) throw new NoSuchElementException();
        long lastId = RecordNumberBinding.entryToRecordNumber(key);
        
        // Get a random element
        cursor = graph.vertexDb.openCursor(null, null);
        RecordNumberBinding.recordNumberToEntry((long)(1 + lastId * Math.random()), key);
        status = cursor.getSearchKeyRange(key, data, null);
        cursor.close();
        if (status == OperationStatus.NOTFOUND) throw new InternalError();
        
        return new BdbVertex(graph, key);
    }

    protected void remove() throws DatabaseException {
    	// Remove linked edge records.
        for (Edge e : this.getEdges(Direction.BOTH))
        	((BdbEdge) e).remove();

    	// Remove properties and vertex record.
    	this.graph.vertexPropertyDb.delete(null, this.id);
    	this.graph.vertexDb.delete(null, this.id);

        this.id = null;
        this.graph = null;
    }
    
    public Object getId() {
    	return id != null ? RecordNumberBinding.entryToRecordNumber(this.id) : null;
    }

    public Object getProperty(final String pkey) {
    	Cursor cursor;
    	OperationStatus status;
    	DatabaseEntry key = graph.key;
    	DatabaseEntry data = graph.data;
    	StringBinding.stringToEntry(pkey, data);
    	
        try {
        	cursor = this.graph.vertexPropertyDb.openCursor(null,  null);
        	
        	status = cursor.getSearchBothRange(this.id, data, null);
        	if (status == OperationStatus.SUCCESS) {
        		
        		key.setPartial(0, 0, true);
        		status = cursor.getCurrent(key, data, null);
        		key.setPartial(false);
        	}
        	
        	cursor.close();
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
        
        if (status != OperationStatus.SUCCESS)
        	return null;
        
        BdbPropertyData result = BdbElement.propertyDataBinding.entryToObject(data);
        return pkey.equals(result.pkey) ? result.value : null;
    }

    public Set<String> getPropertyKeys() {
    	Cursor cursor;
    	OperationStatus status;
    	BdbPropertyData result;
		Set<String> ret = new HashSet<String>();
    	DatabaseEntry key = graph.key;
    	DatabaseEntry data = graph.data;
		
		try {
			cursor = this.graph.vertexPropertyDb.openCursor(null, null);
			
			status = cursor.getSearchKey(this.id, data, null);
			key.setPartial(0, 0, true);
			while (status == OperationStatus.SUCCESS) {
				
				result = BdbElement.propertyDataBinding.entryToObject(data);
				ret.add(result.pkey);
				status = cursor.getNextDup(key, data, null);
			}
			key.setPartial(false);
			
			cursor.close();
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		return ret;
    }
    
    public void setProperty(final String pkey, final Object value) {
    	if (pkey == null || pkey.equals("") || pkey.equals("id"))
    		throw new IllegalArgumentException("BdbVertex: " + pkey + " is an invalid property key.");  		
    	
    	Cursor cursor;
    	OperationStatus status;
    	DatabaseEntry key = graph.key;
    	DatabaseEntry data = graph.data;
    	StringBinding.stringToEntry(pkey, data);
    	BdbPropertyData pdata;
    	
        try {
            //graph.autoStartTransaction();
        	
        	cursor = this.graph.vertexPropertyDb.openCursor(null,  null);
        	
        	// If pkey exists, delete it.
        	status = cursor.getSearchBothRange(this.id, data, null);
        	if (status == OperationStatus.SUCCESS) {
        		
        		key.setPartial(0, 0, true);
        		status = cursor.getCurrent(key, data, null);
        		key.setPartial(false);
        		if (status == OperationStatus.SUCCESS) {
        			
        			pdata = BdbElement.propertyDataBinding.entryToObject(data);
        			if (pkey.equals(pdata.pkey))
        				cursor.delete();
        		} else
        			pdata = new BdbPropertyData();
        	} else
        		pdata = new BdbPropertyData();
        	
        	// Put the new pkey and value.
        	pdata.pkey = pkey;
        	pdata.value = value;
        	BdbElement.propertyDataBinding.objectToEntry(pdata, data);
        	status = cursor.put(this.id, data);
        	
        	cursor.close();
        	
        	//graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
			throw e;
		} catch (Exception e) {
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
			throw new RuntimeException(e.getMessage(), e);
		}
        
        if (status != OperationStatus.SUCCESS)
        	throw new RuntimeException("BdbVertex: Could not set property '" + pkey + "'.");
    }

    public Object removeProperty(final String pkey) {
    	Cursor cursor;
    	OperationStatus status;
    	DatabaseEntry key = graph.key;
    	DatabaseEntry data = graph.data;
    	StringBinding.stringToEntry(pkey, data);
    	BdbPropertyData result = null;
    	
        try {
            //graph.autoStartTransaction();
        	
        	cursor = this.graph.vertexPropertyDb.openCursor(null,  null);
        	
        	// If pkey exists, delete it.
        	if (cursor.getSearchBothRange(this.id, data, null) == OperationStatus.SUCCESS) {
        		
        		key.setPartial(0, 0, false);
        		status = cursor.getCurrent(new DatabaseEntry(), data, null);
        		key.setPartial(true);
        		if (status == OperationStatus.SUCCESS) {
        			
        			result = BdbElement.propertyDataBinding.entryToObject(data);
        			if (pkey.equals(result.pkey))
        				cursor.delete();
        		}
        	}
        	
        	cursor.close();
        	
        	//graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
			throw e;
		} catch (Exception e) {
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
			throw new RuntimeException(e.getMessage(), e);
		}
        
        return result != null ? result.value : null;
    }
    
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        final BdbVertex other = (BdbVertex) obj;
        return this.id.equals(other.id);
    }

    public int hashCode() {
        return this.id.hashCode();
    }
    
    public String toString() {
        return StringFactory.vertexString(this);
    }

	@Override
	public Iterable<Edge> getEdges(Direction direction, String... labels) {
    	if (labels.length == 0) {
    		switch (direction) {
    		case OUT :
    		case IN  :
        		return new BdbEdgeVertexSequence(this.graph, this.id, direction);
    		case BOTH:
    			ArrayList<Iterable<Edge>> a = new ArrayList<Iterable<Edge>>();
    			a.add(new BdbEdgeVertexSequence(this.graph, this.id, Direction.OUT));
    			a.add(new BdbEdgeVertexSequence(this.graph, this.id, Direction.IN ));
    			return new MultiIterable<Edge>(a);
    		default  :
    			throw new IllegalArgumentException("Invalid direction");
    		}
    	}
    	else {
    		switch (direction) {
    		case OUT :
    		case IN  :
        		return new BdbEdgeVertexLabelSequence(this.graph, this.id, direction, labels);
    		case BOTH:
    			ArrayList<Iterable<Edge>> a = new ArrayList<Iterable<Edge>>();
    			a.add(new BdbEdgeVertexLabelSequence(this.graph, this.id, Direction.OUT, labels));
    			a.add(new BdbEdgeVertexLabelSequence(this.graph, this.id, Direction.IN , labels));
    			return new MultiIterable<Edge>(a);
    		default  :
    			throw new IllegalArgumentException("Invalid direction");
    		}
    	}
	}

	@Override
	public Iterable<Vertex> getVertices(Direction direction, String... labels) {
    	if (labels.length == 0) {
    		switch (direction) {
    		case OUT :
    		case IN  :
        		return new BdbVertexVertexSequence(this.graph, this.id, direction);
    		case BOTH:
    			ArrayList<Iterable<Vertex>> a = new ArrayList<Iterable<Vertex>>();
    			a.add(new BdbVertexVertexSequence(this.graph, this.id, Direction.OUT));
    			a.add(new BdbVertexVertexSequence(this.graph, this.id, Direction.IN ));
    			return new MultiIterable<Vertex>(a);
    		default  :
    			throw new IllegalArgumentException("Invalid direction");
    		}
    	}
    	else {
    		switch (direction) {
    		case OUT :
    		case IN  :
        		return new BdbVertexVertexLabelSequence(this.graph, this.id, direction, labels);
    		case BOTH:
    			ArrayList<Iterable<Vertex>> a = new ArrayList<Iterable<Vertex>>();
    			a.add(new BdbVertexVertexLabelSequence(this.graph, this.id, Direction.OUT, labels));
    			a.add(new BdbVertexVertexLabelSequence(this.graph, this.id, Direction.IN , labels));
    			return new MultiIterable<Vertex>(a);
    		default  :
    			throw new IllegalArgumentException("Invalid direction");
    		}
    	}
	}

	@Override
	public Query query() {
		throw new UnsupportedOperationException();
	}
}
