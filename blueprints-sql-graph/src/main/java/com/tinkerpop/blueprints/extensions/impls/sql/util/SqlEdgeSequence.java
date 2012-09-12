package com.tinkerpop.blueprints.extensions.impls.sql.util;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.extensions.impls.sql.SqlGraph;
import com.tinkerpop.blueprints.extensions.impls.sql.SqlEdge;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.sql.*;

/**
 * @author Elaine Angelino (http://www.eecs.harvard.edu/~elaine)
 * @author Peter Macko (http://www.eecs.harvard.edu/~pmacko)
 */
public class SqlEdgeSequence implements Iterable<Edge> {

    private SqlGraph graph = null;
    private long vid = -1;
    private Direction direction = null;
    private String[] labels = null;
    private SqlEdgeSequenceIterator iterator = null;
    
    private boolean keyValue = false;
    private String key = null;
    private Object value = null;
     
    public SqlEdgeSequence(final SqlGraph graph) {
        this.graph = graph;
    }
    
    public SqlEdgeSequence(final SqlGraph graph, String key, Object value) {
    	if (key == null) throw new IllegalArgumentException("key is null");
    	this.graph = graph;
    	this.keyValue = true;
    	this.key = key;
    	this.value = value;
    }

    public SqlEdgeSequence(final SqlGraph graph, final long vid, final Direction direction) {
    	if (direction == null) throw new IllegalArgumentException("direction is null");
        this.graph = graph;
        this.direction = direction;
    	this.vid = vid;
    }
    
    public SqlEdgeSequence(final SqlGraph graph, final long vid, final Direction direction, final String[] labels) {
    	if (direction == null) throw new IllegalArgumentException("direction is null");
        this.graph = graph;
    	this.direction = direction;
    	this.vid = vid;
    	this.labels = labels;	// TODO Should make a copy of the labels
    }

    public Iterator<Edge> iterator() {
    	
    	if (keyValue) {
    		iterator = new SqlEdgeSequenceIterator(key, value);
    	}
    	else if (direction == null) {
    		iterator = new SqlEdgeSequenceIterator();
    	}
    	else if (labels == null) {
    		iterator = new SqlEdgeSequenceIterator(vid, direction);
    	}
    	else {
    		iterator = new SqlEdgeSequenceIterator(vid, direction, labels);
    	}
    	
    	return iterator;
    }

    
    class SqlEdgeSequenceIterator implements Iterator<Edge> {

        private Statement statement = null;
        private ResultSet rs = null;
        private boolean hasNext = false;
        
        byte[] filterByte5 = null;
	    	
	    public SqlEdgeSequenceIterator() {
	        try {
	        	this.statement = graph.connection.createStatement();
	        	//this.statement.setFetchSize(Integer.MIN_VALUE);
	        	this.rs = this.statement.executeQuery("select * from "+graph.getActualNamePrefix()+"edge");
	        	this.hasNext = nextResult();
	        } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	    }
	    
	    public SqlEdgeSequenceIterator(String key, Object value) {
	    	PreparedStatement statement;
	    	
	        try {
	        	
	        	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	        	ObjectOutputStream oos = new ObjectOutputStream(baos);
	        	oos.writeObject(value);
	        	
	        	statement = graph.getEdgesByPropertyStatement;
	        	
	        	statement.setString(1, key);
	        	filterByte5 = baos.toByteArray();
	        	
	        	this.rs = statement.executeQuery();
	        	this.hasNext = nextResult();
	        	
	        } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	        
	        this.statement = null;
	    }

	    public SqlEdgeSequenceIterator(final long vid, final Direction direction) {
	    	PreparedStatement statement;
	    	
	        try {
	        	
	        	switch (direction) {
	        		case OUT : statement = graph.getOutEdgesStatement ; break;
	        		case IN  : statement = graph.getInEdgesStatement  ; break;
	        		case BOTH: statement = graph.getBothEdgesStatement; break;
	        		default  : throw new UnsupportedOperationException();
	        	}
	        	
	        	statement.setLong(1, vid);
	        	if (direction == Direction.BOTH) statement.setLong(2, vid);
	        	
	        	this.rs = statement.executeQuery();
	        	this.hasNext = nextResult();
	        	
	        } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	        
	        this.statement = null;
	    }
	    
	    public SqlEdgeSequenceIterator(final long vid, final Direction direction, final String[] labels) {
	    	PreparedStatement statement;
	    	StringBuilder sb = new StringBuilder("'");
	    	sb.append(labels[0]);
	    	sb.append("'");
	    	for (int i = 1; i < labels.length; ++i) {
	    		sb.append(',');
	    		sb.append("'");
	    		sb.append(labels[i]);
	    		sb.append("'");
	    	}	
	
	        try {
	        	
	        	switch (direction) {
	        		case OUT :
	        			statement = graph.connection.prepareStatement("select * from "+graph.getActualNamePrefix()+"edge where outid=? and label in (" + sb + ")");
	        			break;
	        		case IN  :
	        			statement = graph.connection.prepareStatement("select * from "+graph.getActualNamePrefix()+"edge where inid=? and label in (" + sb + ")");
	        			break;
	        		case BOTH:
		    			statement = graph.connection.prepareStatement("(select * from "+graph.getActualNamePrefix()+"edge where outid=? and label in (" + sb + ")) union all "
		    							+ "(select * from "+graph.getActualNamePrefix()+"edge where inid=? and label in (" + sb + "))");
		    			break;
		       		default  : throw new UnsupportedOperationException();
	        	}
	        	
	        	statement.setLong(1, vid);
	        	if (direction == Direction.BOTH) statement.setLong(2, vid);
	        	
	        	this.rs = statement.executeQuery();
	        	this.hasNext = nextResult();
	        	
	        } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	        
	        this.statement = statement;
	    }
		
		public Edge next() {
			Edge result = null;
			
	        try {
	        	if (this.hasNext) {
	        		result = new SqlEdge(
	        				graph,
	        				this.rs.getLong(1),
	        				this.rs.getLong(2),
	        				this.rs.getLong(3),
	        				this.rs.getString(4));
	        		this.hasNext = nextResult();
	        	} else {
	        		this.rs.close();
	        		if (this.statement != null) this.statement.close();
	        	}
	        } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	        
	        if (result == null)
	        	throw new NoSuchElementException();
	        
	        return result;
		}
	
		public boolean hasNext() {
			if (!this.hasNext) {
		        try {
	        		this.rs.close();
	        		if (this.statement != null) this.statement.close();
		        } catch (RuntimeException e) {
		            throw e;
		        } catch (Exception e) {
		            throw new RuntimeException(e.getMessage(), e);
		        }
			}
	    	return this.hasNext;
	    }
		
		public void close() {
	        try {
	        	this.rs.close();
	        	if (this.statement != null) this.statement.close();
	        	hasNext = false;
	        } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
		}
	
	    public void remove() { 
	        throw new UnsupportedOperationException(); 
	    } 
	    
	    protected boolean filterCurrent() throws SQLException {
	    	
	    	if (filterByte5 != null) {
	    		byte[] b = rs.getBytes(5);
	    		return Arrays.equals(b, filterByte5);
	    	}
	    	
	    	return true;
	    }
	    
	    
	    protected boolean nextResult() throws SQLException {
	    	
	    	while (rs.next()) {
	    		if (filterCurrent()) return true; 
	    	}
	    	
	    	return false;
	    }
   }
}