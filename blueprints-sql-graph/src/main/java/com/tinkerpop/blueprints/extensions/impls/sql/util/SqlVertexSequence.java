package com.tinkerpop.blueprints.extensions.impls.sql.util;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.impls.sql.SqlGraph;
import com.tinkerpop.blueprints.extensions.impls.sql.SqlVertex;

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
public class SqlVertexSequence implements CloseableIterable<Vertex> {

    private SqlGraph graph = null;
    
    private long vid = -1;
    private Direction direction = null;
    private String[] labels = null;
    
    private boolean dijkstra = false;
    private long source = -1;
    private long target = -1;
    
    private boolean keyValue = false;
    private String key = null;
    private Object value = null;
    
    private AbstractSqlVertexSequenceIterator iterator = null;
    
    
    protected SqlVertexSequence(final SqlGraph graph) {
    	this.graph = graph;
    }
    
    public SqlVertexSequence(final SqlGraph graph, String key, Object value) {
    	if (key == null) throw new IllegalArgumentException("key is null");
    	this.graph = graph;
    	this.keyValue = true;
    	this.key = key;
    	this.value = value;
    }
    
    public SqlVertexSequence(final SqlGraph graph, long vid, Direction direction) {
    	if (direction == null) throw new IllegalArgumentException("direction is null");
        this.graph = graph;
        this.direction = direction;
    	this.vid = vid;
    }
    
    public SqlVertexSequence(final SqlGraph graph, long vid, Direction direction, String[] labels) {
    	if (direction == null) throw new IllegalArgumentException("direction is null");
        this.graph = graph;
    	this.direction = direction;
    	this.vid = vid;
    	this.labels = labels;	// TODO Should make a copy of the labels
    }
    
    public static SqlVertexSequence getAllVertices(final SqlGraph graph) {
    	return new SqlVertexSequence(graph);
    }
    
    public static SqlVertexSequence dijkstra(final SqlGraph graph, final long source, final long target) {
    	
    	SqlVertexSequence s = new SqlVertexSequence(graph);
    	s.dijkstra = true;
    	s.source = source;
    	s.target = target;
   	
    	return s;
    }

    public Iterator<Vertex> iterator() {

    	if (dijkstra) {
    		iterator = new DijkstraIterator(source, target);
    	}
    	if (keyValue) {
    		iterator = new SqlVertexSequenceIterator(key, value);
    	}
    	else if (direction == null) {
    		iterator = new SqlVertexSequenceIterator();
    	}
    	else if (labels == null) {
    		iterator = new SqlVertexSequenceIterator(vid, direction);
    	}
    	else {
    		iterator = new SqlVertexSequenceIterator(vid, direction, labels);
    	}
    	
    	return iterator;
    }
	
    @Override
	public void close() {
    	if (iterator != null) iterator.close();
	}
    
    
    abstract class AbstractSqlVertexSequenceIterator implements Iterator<Vertex> {
    	
        protected Statement statement = null;
        protected ResultSet rs = null;
        protected boolean hasNext = false;
        protected boolean open = true;
        
        protected AbstractSqlVertexSequenceIterator() {
        	//
        }
		
		public Vertex next() {
			Vertex result = null;
			
	        try {
	        	if (this.hasNext) {
	        		result = new SqlVertex(graph, this.rs.getLong(1));
	        		this.hasNext = nextResult();
	        	} else {
	        		this.rs.close();
	        		if (this.statement != null) this.statement.close();
	        		open = false;
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
	        		open = false;
		        } catch (RuntimeException e) {
		            throw e;
		        } catch (Exception e) {
		            throw new RuntimeException(e.getMessage(), e);
		        }
			}
	    	return this.hasNext;
	    }
		
		public void close() {
			if (!open) return;
	        try {
	        	this.rs.close();
	        	if (this.statement != null) this.statement.close();
	        	open = false;
	        } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
		}
	
	    public void remove() { 
	        throw new UnsupportedOperationException(); 
	    }
	    
	    protected abstract boolean filterCurrent() throws SQLException;
	    
	    protected boolean nextResult() throws SQLException {
	    	
	    	while (rs.next()) {
	    		if (filterCurrent()) return true; 
	    	}
	    	
	    	return false;
	    }
    }

    
    class SqlVertexSequenceIterator extends AbstractSqlVertexSequenceIterator {
    	
    	byte[] filterByte2 = null;
	
        public SqlVertexSequenceIterator() {
	    	
	        try {
	        	statement = graph.connection.createStatement();
	        	//s.statement.setFetchSize(Integer.MIN_VALUE);
	        	rs = statement.executeQuery("select * from "+graph.getActualNamePrefix()+"vertex");
	        	hasNext = nextResult();
	        } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	    }
	    
	    public SqlVertexSequenceIterator(String key, Object value) {
	    	PreparedStatement statement;
	    	
	        try {
	        	
	        	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	        	ObjectOutputStream oos = new ObjectOutputStream(baos);
	        	oos.writeObject(value);
	        	
	        	statement = graph.getVerticesByPropertyStatement;
	        	
	        	statement.setString(1, key);
	        	filterByte2 = baos.toByteArray();
	        	
	        	this.rs = statement.executeQuery();
	        	this.hasNext = nextResult();
	        	
	        } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	        
	        this.statement = null;
	    }
	    
	    public SqlVertexSequenceIterator(long vid, Direction direction) {
	    	PreparedStatement statement;
	    	
	        try {
	        	
	        	switch (direction) {
	        		case OUT : statement = graph.getOutVerticesStatement ; break;
	        		case IN  : statement = graph.getInVerticesStatement  ; break;
	        		case BOTH: statement = graph.getBothVerticesStatement; break;
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
	    
	    public SqlVertexSequenceIterator(long vid, Direction direction, String[] labels) {

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
	        			statement = graph.connection.prepareStatement("select inid, eid from "+graph.getActualNamePrefix()+"edge where outid=? and label in (" + sb + ")");
	        			break;
	        		case IN  :
	        			statement = graph.connection.prepareStatement("select outid, eid from "+graph.getActualNamePrefix()+"edge where inid=? and label in (" + sb + ")");
	        			break;
	        		case BOTH:
		    			statement = graph.connection.prepareStatement("(select inid as id, eid from "+graph.getActualNamePrefix()+"edge where outid=? and label in (" + sb + ")) union all "
		    							+ "(select outid as id, eid from "+graph.getActualNamePrefix()+"edge where inid=? and label in (" + sb + "))");
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
	    
	    protected boolean filterCurrent() throws SQLException {
	    	
	    	if (filterByte2 != null) {
	    		byte[] b = rs.getBytes(2);
	    		return Arrays.equals(b, filterByte2);
	    	}
	    	
	    	return true;
	    }
    }
	    

    
    class DijkstraIterator extends AbstractSqlVertexSequenceIterator {
    	
	    public DijkstraIterator(final long source, final long target) {

	    	PreparedStatement statement = null;
	    	
	    	try {
	    		statement = graph.connection.prepareCall("call "+graph.getActualNamePrefix()+"dijkstra(?,?)");
	    		statement.setLong(1, source);
	    		statement.setLong(2, target);
	    		rs = statement.executeQuery();
	    		hasNext = nextResult();
	    	} catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	    	
	    	this.statement = statement;
	    }
	    
	    protected boolean filterCurrent() throws SQLException {
	    	return true;
	    }
    }
}
