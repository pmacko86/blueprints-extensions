package com.tinkerpop.blueprints.extensions.impls.sql;

import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import com.tinkerpop.blueprints.extensions.io.fgf.FGFTypes;


/**
 * A class for accessing vertex and edge properties
 * 
 * TODO Still not in use -- switch to this implementation.
 * 
 * @author Peter Macko (http://www.eecs.harvard.edu/~pmacko)
 */
public class SqlProperty {

	private static Pattern propertyNamePattern = Pattern.compile("^[_a-zA-Z][_a-zA-Z0-9]*$");
	

	/**
	 * Property name
	 */
	private String name;
	
	/**
	 * Property data type
	 */
	private short type;
	
	/**
	 * Vertex or edge?
	 */
	private SqlElement.Type elementType;
	
	/**
	 * Graph
	 */
	private SqlGraph graph;
	
	/**
	 * JDBC connection
	 */
	private Connection connection;
	
	/**
	 * The table name
	 */
	private String table;
	
	/**
	 * The ID column
	 */
	private String idc;
	
	/**
	 * The get property value statement
	 */
	private PreparedStatement getPropertyStatement;
	
	/**
	 * The get elements by property value statement
	 */
	private PreparedStatement getElementsStatement;
	
	/**
	 * The set (insert or update) property statement 
	 */
	private PreparedStatement setPropertyStatement;
	
	/**
	 * The delete property statement 
	 */
	private PreparedStatement deletePropertyStatement;

	
	/**
	 * Create an instance of SqlProperty
	 * 
	 * @param graph the graph
	 * @param elementType whether this is a vertex or an edge property
	 * @param type the data type as a FGF constant (if the property does not yet exist)
	 * @param name the property name (key)
	 * @throws SQLException on error
	 */
	public SqlProperty(SqlGraph graph, SqlElement.Type elementType, short type, String name) throws SQLException {
		
		if (!propertyNamePattern.matcher(name).matches()) {
			throw new RuntimeException("Invalid property name (can contain only letters, "
					+ "numbers, and _, and has to start with a letter or _)");
		}
		
		this.graph = graph;
		this.connection = graph.connection;
		this.elementType = elementType;
		this.type = type;
		this.name = name;
		
		this.idc = this.elementType == SqlElement.Type.VERTEX ? "vid" : "eid";
		this.table = this.graph.getActualNamePrefix() + (this.elementType == SqlElement.Type.VERTEX ? "vertex" : "edge");
		
		
		// Check if the property exists, and if not, create it
		
		DatabaseMetaData metadata = connection.getMetaData();
		ResultSet resultSet = metadata.getColumns(null, null, table, name);
		boolean exists = resultSet.next();
		resultSet.close();
		
		if (!exists) {
			
			String sqlType = null;
			switch (this.type) {
			case FGFTypes.BOOLEAN: sqlType = "BOOLEAN"     ; break;
			case FGFTypes.DOUBLE : sqlType = "DOUBLE"      ; break;
			case FGFTypes.FLOAT  : sqlType = "FLOAT"       ; break;
			case FGFTypes.INTEGER: sqlType = "INT"         ; break;
			case FGFTypes.LONG   : sqlType = "BIGINT"      ; break;
			case FGFTypes.OTHER  : sqlType = "BLOB"        ; break;
			case FGFTypes.SHORT  : sqlType = "SMALLINT"    ; break;
			case FGFTypes.STRING : sqlType = "VARCHAR(255)"; break;
			default:
				throw new IllegalArgumentException("Unsupported property type");
			}
			
			Statement statement = this.connection.createStatement();
			statement.executeUpdate("ALTER TABLE "+table+" ADD "+this.name+" " + sqlType);
			statement.close();
		}
		
		
		// Prepare the statements
		
		this.getPropertyStatement
			= this.connection.prepareStatement("select "+ name+" from "+table+" where "+ idc+"=?");
		this.getElementsStatement
			= this.connection.prepareStatement("select "+ idc +" from "+table+" where "+name+"=?");
		this.setPropertyStatement
			= this.connection.prepareStatement("update "+table+" set "+name+"=? where "+ idc+"=?");
		this.deletePropertyStatement
			= this.connection.prepareStatement("update "+table+" set "+name+"=NULL where "+ idc+"=?");
	}
	
	
	/**
	 * Get the set of property keys
	 * 
	 * @param graph the graph
	 * @param elementType whether this is a vertex or an edge property
	 * @return the set of property keys
	 */
	public static Set<String> getPropertyKeys(SqlGraph graph, SqlElement.Type elementType) {
		Set<String> result = new HashSet<String>();
		
		try {
    		Connection connection = graph.connection;
        	String table = graph.getActualNamePrefix() + (elementType == SqlElement.Type.VERTEX ? "vertex" : "edge");
        	
        	DatabaseMetaData metadata = connection.getMetaData();
    		ResultSet resultSet = metadata.getColumns(null, null, table, null);
    		
    		// TODO Implement this
    		
    		resultSet.close();
			
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		return result;
	}
	
	
	/**
	 * Get a property value
	 * 
	 * @param element the vertex or the edge
	 * @return the value, or null if not there
	 */
	public Object getValue(SqlElement element) {
		
		Object result = null;
    	
    	try {
    		getPropertyStatement.setLong(1, element.getRawId());
    		ResultSet rs = getPropertyStatement.executeQuery();

        	if (rs.next()) {
        		if (type == FGFTypes.OTHER) {
        			ObjectInputStream ois = new ObjectInputStream(rs.getBinaryStream(1));
            		result = ois.readObject();
        		}
        		else {
        			result = rs.getObject(1);
        		}
        	}
        	
        	rs.close();
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		return result;
	}
	
	
	/**
	 * Set a property value
	 * 
	 * @param element the vertex or the edge
	 * @param value the new value
	 */
	public void setValue(SqlElement element, Object value) {
		
    	try {
       		setPropertyStatement.setLong(1, element.getRawId());
    		
    		if (type == FGFTypes.OTHER) {
    			ByteArrayOutputStream baos = new ByteArrayOutputStream();
    	    	ObjectOutputStream oos = new ObjectOutputStream(baos);
    	    	oos.writeObject(value);
    	    	setPropertyStatement.setBytes(3, baos.toByteArray());
    		}
    		else {
    			setPropertyStatement.setObject(2, value);
    		}

            graph.autoStartTransaction();
    		setPropertyStatement.executeUpdate();
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	
	/**
	 * Delete the property
	 * 
	 * @param element the vertex or the edge
	 */
	public void delete(SqlElement element) {
		
    	try {
       		deletePropertyStatement.setLong(1, element.getRawId());
       		
    		graph.autoStartTransaction();
      		deletePropertyStatement.executeUpdate();
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	
	/**
	 * Return a prepared statement (that should not be closed) configured to return IDs
	 * matching all objects with the given property value
	 * 
	 * @param element the vertex or the edge
	 * @param value the property value
	 * @throws SQLException on error
	 */
	public PreparedStatement getVerticesStatement(SqlElement element, Object value) throws SQLException {
		
		if (type == FGFTypes.OTHER) {
			throw new UnsupportedOperationException("Unsupported for type FGFTypes.OTHER");
		}
		
    	getElementsStatement.setLong(1, element.getRawId());
    	getElementsStatement.setObject(2, value);
    	return getElementsStatement;
	}
}
