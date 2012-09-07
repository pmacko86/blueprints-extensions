package com.tinkerpop.blueprints.extensions.impls.sql;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.BenchmarkableGraph;
import com.tinkerpop.blueprints.extensions.BulkloadableGraph;
import com.tinkerpop.blueprints.extensions.impls.sql.util.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;


/**
 * A Blueprints implementation of an SQL database (http://www.oracle.com)
 *
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 * @author Peter Macko (http://www.eecs.harvard.edu/~pmacko)
 */
@SuppressWarnings("deprecation")
public class SqlGraph implements TransactionalGraph, BulkloadableGraph, BenchmarkableGraph {
	
	// Note: This code is not thread-safe, but there is no need to make it as such, since
	// MySQL performs much better if it is used from two connections from two threads, rather
	// than from a single connection shared by two (or more) threads.
	
	private String addr = null;
	private String namePrefix = null;
	private String orgNamePrefix = null;
	private String databaseName = null;
	public Connection connection = null;
	
	private static Semaphore initSemaphore = new Semaphore(1);
	private static HashMap<String, Integer> numConnectionsPerAddr = new HashMap<String, Integer>();
	private static HashMap<String, List<SqlGraph>> connectionsPerAddr = new HashMap<String, List<SqlGraph>>();
	private StackTraceElement[] constructorStackTrace;
	
	protected boolean autoCommit = false;
	
    private final ThreadLocal<Boolean> tx = new ThreadLocal<Boolean>() {
        protected Boolean initialValue() {
            return false;
        }
    };
    private final ThreadLocal<Integer> txBuffer = new ThreadLocal<Integer>() {
        protected Integer initialValue() {
            return 1;
        }
    };
    private final ThreadLocal<Integer> txCounter = new ThreadLocal<Integer>() {
        protected Integer initialValue() {
            return 0;
        }
    };
    
	private boolean bulkLoadMode = false;
	
	public PreparedStatement getOutEdgesStatement;
	public PreparedStatement getInEdgesStatement;
	public PreparedStatement getBothEdgesStatement;
	
	public PreparedStatement getOutVerticesStatement;
	public PreparedStatement getInVerticesStatement;
	public PreparedStatement getBothVerticesStatement;
	
	
	// Features

    private static final Features FEATURES = new Features();

    static {
    	
    	// TODO We need to revisit these

        FEATURES.supportsSerializableObjectProperty = false;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = false;
        FEATURES.supportsUniformListProperty = false;
        FEATURES.supportsMixedListProperty = false;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = false;
        FEATURES.supportsStringProperty = true;

        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = true;
        FEATURES.isWrapper = false;
        FEATURES.isRDFModel = false;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = true;
        FEATURES.supportsEdgeIndex = true;
        FEATURES.ignoresSuppliedIds = true;
        FEATURES.supportsTransactions = true;
        FEATURES.supportsIndices = false;
        FEATURES.supportsKeyIndices = false;
        FEATURES.supportsVertexKeyIndex = false;
        FEATURES.supportsEdgeKeyIndex = false;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsThreadedTransactions = false;
    }


	
    /**
     * Creates a new instance of a SqlGraph.
     *
     * @param addr The database connection address.
     */
    public SqlGraph(final String addr) {
    	this(addr.contains("|") ? addr.substring(0, addr.indexOf('|')) : addr,
    			addr.contains("|") ? addr.substring(addr.indexOf('|') + 1) : "");
    }
    
    /**
     * Creates a new instance of a SqlGraph.
     *
     * @param addr The database connection address.
     * @param databaseName The database name.
     */
    public SqlGraph(final String addr, String databaseName) {
    	this(addr, databaseName, null);
    }

    /**
     * Creates a new instance of a SqlGraph.
     *
     * @param addr The database connection address.
     * @param databaseName The database name.
     * @param namePrefix The prefix for the table names.
     */
    public SqlGraph(final String addr, String databaseName, String namePrefix) {
    	this.addr = addr;
    	
    	if (namePrefix == null) namePrefix = "";
    	if (databaseName == null) databaseName = "";
    	
    	this.databaseName = databaseName;
    	this.orgNamePrefix = namePrefix;
    	
    	if (!namePrefix.equals("")) {
	    	if (!Pattern.matches("^[a-z][a-z0-9_]*$", namePrefix)) {
	    		throw new IllegalArgumentException("Invalid name prefix (can contain only lower-case letters, "
	    				+ "numbers, and _, and has to start with a letter)");
	    	}
	    	namePrefix += "_";
    	}
    	this.namePrefix = namePrefix;
    	
    	try {
    		initSemaphore.acquire();
   		
        	Class.forName("com.mysql.jdbc.Driver").newInstance();
            this.connection = DriverManager.getConnection(
            		"jdbc:mysql:" + this.addr + "&autoDeserialize=true");
        	if (!databaseName.equals("")) {
            	connection.setCatalog(databaseName);
        	}
        	
            Statement statement = this.connection.createStatement();
            
            String addrExt = toString();
            if (!numConnectionsPerAddr.containsKey(addrExt)) {
            	numConnectionsPerAddr.put(addrExt, 0);
            }
            if (!connectionsPerAddr.containsKey(addrExt)) {
            	connectionsPerAddr.put(addrExt, new ArrayList<SqlGraph>());
            }


            //
            // Set up the database schema and shared procedures
            //
            
            if (numConnectionsPerAddr.get(addrExt) <= 0) {
	            
	            // First, some MySQL housekeeping.
	            statement.executeUpdate(
	            		"DROP PROCEDURE IF EXISTS create_index_if_not_exists");
	        	statement.executeUpdate(
	        			"CREATE PROCEDURE create_index_if_not_exists(theIndex VARCHAR(128), theTable VARCHAR(128), theColumn VARCHAR(129))\r\n" + 
	        			"BEGIN\r\n" + 
	        			"    IF NOT EXISTS (\r\n" + 
	        			"        SELECT *\r\n" + 
	        			"        FROM information_schema.statistics\r\n" + 
	        			"        WHERE table_schema = database()\r\n" + 
	        			"        AND table_name = theTable\r\n" + 
	        			"        AND index_name = theIndex)\r\n" + 
	        			"    THEN\r\n" + 
	        			"        SET @s = CONCAT('CREATE INDEX ',theIndex,' ON ',theTable,' (',theColumn,')');\r\n" + 
	        			"        PREPARE stmt FROM @s;\r\n" + 
	        			"        EXECUTE stmt;\r\n" + 
	        			"    END IF;\r\n" + 
	        			"END;");
	            
	            // Create structure tables.
	            statement.executeUpdate(
	            		"create table if not exists "+namePrefix+"vertex(" +
	            			"vid serial primary key)");
	            
	            statement.executeUpdate(
	            		"create table if not exists "+namePrefix+"edge(" +
		            		"eid serial primary key," +
		            		"outid bigint unsigned not null references "+namePrefix+"vertex(vid)," +
		            		"inid bigint unsigned not null references "+namePrefix+"vertex(vid)," +
		            		"label varchar(255))");
	            this.connection.prepareCall(
	        			"call create_index_if_not_exists('"+namePrefix+"outid_index', '"+namePrefix+"edge', 'outid')").executeUpdate();
	            this.connection.prepareCall(
						"call create_index_if_not_exists('"+namePrefix+"inid_index', '"+namePrefix+"edge', 'inid')").executeUpdate();
	            this.connection.prepareCall(
						"call create_index_if_not_exists('"+namePrefix+"label_index', '"+namePrefix+"edge', 'label')").executeUpdate();
	
	            // Create property tables.
	            statement.executeUpdate(
	            		"create table if not exists "+namePrefix+"vertexproperty(" +
		            		"vid bigint unsigned not null references "+namePrefix+"vertex(vid)," +
		            		"pkey varchar(255) not null," +
		            		"value blob," +
		            		"primary key(vid,pkey))");
	            //this.connection.prepareCall(
				//		"call create_index_if_not_exists('vertexpkey_index', 'vertexproperty', 'pkey')").executeUpdate();
	            
	            statement.executeUpdate(
	            		"create table if not exists "+namePrefix+"edgeproperty(" +
		            		"eid bigint unsigned not null references "+namePrefix+"edge(eid)," +
		            		"pkey varchar(255) not null," +
		            		"value blob," +
		            		"primary key(eid,pkey))");
	            //this.connection.prepareCall(
				//		"call create_index_if_not_exists('edgepkey_index', 'edgeproperty', 'pkey')").executeUpdate();
	        	
	        	// Create the shortest path procedure.
	            statement.executeUpdate("DROP PROCEDURE IF EXISTS "+namePrefix+"dijkstra");
	        	statement.executeUpdate(
	        			"CREATE PROCEDURE "+namePrefix+"dijkstra(sourceid BIGINT UNSIGNED, targetid BIGINT UNSIGNED)\r\n" + 
	        			"BEGIN\r\n" + 
	        			"    DECLARE currid BIGINT UNSIGNED;\r\n" + 
	        			"    DROP TEMPORARY TABLE IF EXISTS "+namePrefix+"paths;\r\n" + 
	        			"    CREATE TEMPORARY TABLE "+namePrefix+"paths (\r\n" + 
	        			"        vid BIGINT UNSIGNED NOT NULL PRIMARY KEY,\r\n" + 
	        			"        calc TINYINT UNSIGNED NOT NULL,\r\n" + 
	        			"        prev BIGINT UNSIGNED\r\n" + 
	        			"    );\r\n" + 
	        			"\r\n" + 
	        			"    INSERT INTO "+namePrefix+"paths (vid, calc, prev) VALUES (sourceid, 0, NULL);\r\n" + 
	        			"    SET currid = sourceid;\r\n" + 
	        			"    WHILE currid IS NOT NULL DO\r\n" + 
	        			"    BEGIN\r\n" + 
	        			"        INSERT IGNORE INTO "+namePrefix+"paths (vid, calc, prev)\r\n" + 
	        			"        SELECT inid, 0, currid\r\n" + 
	        			"        FROM "+namePrefix+"edge\r\n" + 
	        			"        WHERE currid = outid;\r\n" + 
	        			"\r\n" + 
	        			"        UPDATE "+namePrefix+"paths SET calc = 1 WHERE currid = vid;\r\n" + 
	        			"\r\n" + 
	        			"        IF EXISTS (SELECT vid FROM "+namePrefix+"paths WHERE targetid = vid LIMIT 1)\r\n" + 
	        			"        THEN\r\n" + 
	        			"            SET currid = NULL;\r\n" + 
	        			"        ELSE\r\n" + 
	        			"            SET currid = (SELECT vid FROM "+namePrefix+"paths WHERE calc = 0 LIMIT 1);\r\n" + 
	        			"        END IF;\r\n" + 
	        			"    END;\r\n" + 
	        			"    END WHILE;\r\n" + 
	        			"\r\n" + 
	        			"    DROP TEMPORARY TABLE IF EXISTS "+namePrefix+"result;\r\n" + 
	        			"    CREATE TEMPORARY TABLE "+namePrefix+"result (\r\n" + 
	        			"        vid BIGINT UNSIGNED NOT NULL PRIMARY KEY\r\n" + 
	        			"    );\r\n" + 
	        			"\r\n" + 
	        			"    SET currid = targetid;\r\n" + 
	        			"    WHILE currid IS NOT NULL DO\r\n" + 
	        			"    BEGIN\r\n" + 
	        			"        INSERT INTO "+namePrefix+"result (vid) VALUES (currid);\r\n" + 
	        			"        SET currid = (SELECT prev FROM "+namePrefix+"paths WHERE currid = vid LIMIT 1);\r\n" + 
	        			"    END;\r\n" + 
	        			"    END WHILE;\r\n" + 
	        			"\r\n" + 
	        			"    SELECT vid FROM "+namePrefix+"result;\r\n" + 
	        			"END;");
	            
	 	        	statement.close();
            }
            
        	
        	//
        	// Create prepared statements.
        	//
        	
        	// Vertices:
        	
        	addVertexStatement = connection.prepareStatement(
        						"insert into "+SqlGraph.this.namePrefix+"vertex values(default)",
								Statement.RETURN_GENERATED_KEYS);
           	
        	getVertexStatement = connection.prepareStatement(
								"select exists(select * from "+SqlGraph.this.namePrefix+"vertex where vid=?)");
           	
        	getMaxVertexIdStatement = connection.prepareStatement(
        						"select max(vid) from "+SqlGraph.this.namePrefix+"vertex;");
        	getVertexAfterStatement = connection.prepareStatement(
        						"select vid from "+SqlGraph.this.namePrefix+"vertex where vid >= ? order by vid limit 1;");
        	
        	countVerticesStatement = connection.prepareStatement(
								"select count(*) from "+SqlGraph.this.namePrefix+"vertex;");
        	removeVertexStatement = connection.prepareStatement(
								"delete from "+SqlGraph.this.namePrefix+"vertex where vid=?");
        	removeVertexPropertiesStatement = connection.prepareStatement(
								"delete from "+SqlGraph.this.namePrefix+"vertexproperty where vid=?");
        	
        	
        	// Vertex properties:
        	
        	getVertexPropertyStatement = connection.prepareStatement(
								"select value from "+SqlGraph.this.namePrefix+"vertexproperty where vid=? and pkey=?");
        	getVertexPropertyKeysStatement = connection.prepareStatement(
								"select pkey from "+SqlGraph.this.namePrefix+"vertexproperty where vid=?");
        	setVertexPropertyStatement = connection.prepareStatement(
								"replace into "+SqlGraph.this.namePrefix+"vertexproperty values(?,?,?)");
        	removeVertexPropertyStatement = connection.prepareStatement(
								"delete from "+SqlGraph.this.namePrefix+"vertexproperty where vid=? and pkey=?");
        	
        	
        	// Vertex in/out edges:
        	
        	getOutEdgesStatement = connection.prepareStatement(
        						"select * from "+SqlGraph.this.namePrefix+"edge where outid=?");
        	getInEdgesStatement = connection.prepareStatement(
								"select * from "+SqlGraph.this.namePrefix+"edge where inid=?");
        	getBothEdgesStatement = connection.prepareStatement(
								"(select * from "+SqlGraph.this.namePrefix+"edge where outid=?) union all " +
								"(select * from "+SqlGraph.this.namePrefix+"edge where inid=?)");
        	
        	
        	// Vertex in/out vertices:
        	
        	getOutVerticesStatement = connection.prepareStatement(
        						"select inid, eid from "+SqlGraph.this.namePrefix+"edge where outid=?");
        	getInVerticesStatement = connection.prepareStatement(
								"select outid, eid from "+SqlGraph.this.namePrefix+"edge where inid=?");
        	getBothVerticesStatement = connection.prepareStatement(
								"(select outid as id, eid from "+SqlGraph.this.namePrefix+"edge where outid=?) union all " +
								"(select inid  as id, eid from "+SqlGraph.this.namePrefix+"edge where inid=?)");
       	
        	
        	// Edges:
        	
        	getEdgeVerticesStatement = connection.prepareStatement(
								"select exists(select * from "+SqlGraph.this.namePrefix+"vertex where vid=?) " +
								"and exists(select * from "+SqlGraph.this.namePrefix+"vertex where vid=?)");
        	addEdgeStatement = connection.prepareStatement(
								"insert into "+SqlGraph.this.namePrefix+"edge(outid,inid,label) values(?,?,?)",
								Statement.RETURN_GENERATED_KEYS);
        	getEdgeStatement = connection.prepareStatement(
								"select outid,inid,label from "+SqlGraph.this.namePrefix+"edge where eid=?");
        	getMaxEdgeIdStatement = connection.prepareStatement(
								"select max(eid) from "+SqlGraph.this.namePrefix+"edge;");
        	getEdgeAfterStatement = connection.prepareStatement(
								"select eid,outid,inid,label from "+SqlGraph.this.namePrefix+"edge where eid >= ? order by eid limit 1;");
        	countEdgesStatement = connection.prepareStatement(
								"select count(*) from "+SqlGraph.this.namePrefix+"edge;");
        	removeEdgeStatement = connection.prepareStatement(
								"delete from "+SqlGraph.this.namePrefix+"edge where eid=?");
        	removeEdgePropertiesStatement = connection.prepareStatement(
								"delete from "+SqlGraph.this.namePrefix+"edgeproperty where eid=?");
        	
        	
        	// Edge properties:
        	
        	getEdgePropertyStatement = connection.prepareStatement(
								"select value from "+SqlGraph.this.namePrefix+"edgeproperty where eid=? and pkey=?");
        	getEdgePropertyKeysStatement = connection.prepareStatement(
								"select pkey from "+SqlGraph.this.namePrefix+"edgeproperty where eid=?");
        	setEdgePropertyStatement = connection.prepareStatement(
								"replace into "+SqlGraph.this.namePrefix+"edgeproperty values(?,?,?)");
        	removeEdgePropertyStatement = connection.prepareStatement(
								"delete from "+SqlGraph.this.namePrefix+"edgeproperty where eid=? and pkey=?");
        			
        	
        	// Disable auto-commit.
        	
            connection.setAutoCommit(autoCommit);
           
            
            // Finish
            
            constructorStackTrace = Thread.currentThread().getStackTrace();
            numConnectionsPerAddr.put(addrExt, numConnectionsPerAddr.get(addrExt) + 1);
            connectionsPerAddr.get(addrExt).add(this);
            
            initSemaphore.release();

    	} catch (RuntimeException e) {
    		initSemaphore.release();
    		throw e;
    	} catch (Exception e) {
    		initSemaphore.release();
    		throw new RuntimeException(e);
    	}
    }
    
    public static void createDatabase(String addr, String dbName) throws SQLException {
    	// Does this belong here?
    	
    	if (!Pattern.matches("[A-Za-z][A-Za-z0-9_]*", dbName)) {
    		throw new IllegalArgumentException("Invalid database name: " + dbName);
    	}
    	    	
    	try {
    		Class.forName("com.mysql.jdbc.Driver").newInstance();
    	}
    	catch (Exception e) {
    		throw new RuntimeException(e);
    	}
    	
        Connection connection = DriverManager.getConnection(
        		"jdbc:mysql:" + addr + "&autoDeserialize=true");
      
        try {
	        Statement statement = connection.createStatement();
	        statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName);
	        statement.close();
        }
        finally {
        	connection.close();
        }
    }
    
    public static void dropDatabase(String addr, String dbName) throws SQLException {
    	// Does this belong here?
    	
    	if (!Pattern.matches("[A-Za-z][A-Za-z0-9_]*", dbName)) {
    		throw new IllegalArgumentException("Invalid database name: " + dbName);
    	}
  	
    	try {
    		Class.forName("com.mysql.jdbc.Driver").newInstance();
    	}
    	catch (Exception e) {
    		throw new RuntimeException(e);
    	}
    	
        Connection connection = DriverManager.getConnection(
        		"jdbc:mysql:" + addr + "&autoDeserialize=true");
      
        try {
	        Statement statement = connection.createStatement();
	        statement.executeUpdate("DROP DATABASE IF EXISTS " + dbName);
	        statement.close();
        }
        finally {
        	connection.close();
        }
    }
    
    public String getActualNamePrefix() {
    	return namePrefix;
    }
    
    public String getSpecifiedNamePrefix() {
    	return orgNamePrefix;
    }

    // BLUEPRINTS GRAPH INTERFACE
    protected PreparedStatement addVertexStatement;
    public Vertex addVertex(final Object id) {        
        try {
            autoStartTransaction();
            final Vertex vertex = new SqlVertex(this);
            autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
            return vertex;
        } catch (RuntimeException e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }     
    }

    protected PreparedStatement getVertexStatement;
    public Vertex getVertex(final Object id) {
    	if (id == null)
    		throw new IllegalArgumentException("SqlGraph.getVertex(id) cannot be null.");
    	
    	try {
    		return new SqlVertex(this, id);
    	} catch (Exception e) {
    		return null;
    	}
    }

    public Iterable<Vertex> getVertices() {
        return SqlVertexSequence.getAllVertices(this);
    }
    
    protected PreparedStatement countVerticesStatement;
    public long countVertices() {
    	try {
			ResultSet rs = countVerticesStatement.executeQuery();
			rs.next();
			long r = rs.getLong(1);
			rs.close();
			return r;
    	}
    	catch (SQLException e) {
    		throw new RuntimeException(e);
    	}
    }
    
    protected PreparedStatement getMaxVertexIdStatement;
    protected PreparedStatement getVertexAfterStatement;
    public Vertex getRandomVertex() {
    	try {
    		return SqlVertex.getRandomVertex(this);
    	}
    	catch (SQLException e) {
    		throw new RuntimeException(e);
    	}
    }

    protected PreparedStatement removeVertexStatement;
    protected PreparedStatement removeVertexPropertiesStatement;
    public void removeVertex(final Vertex vertex) {
        if (vertex == null || vertex.getId() == null)
            return;
        
        try {
            autoStartTransaction();
            ((SqlVertex) vertex).remove();
            autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    protected PreparedStatement getVertexPropertyStatement;
    protected PreparedStatement getVertexPropertyKeysStatement;
    protected PreparedStatement setVertexPropertyStatement;
    protected PreparedStatement removeVertexPropertyStatement;

    protected PreparedStatement getEdgeVerticesStatement;
    protected PreparedStatement addEdgeStatement;
    public Edge addEdge(
		final Object id,
		final Vertex outVertex,
		final Vertex inVertex,
		final String label)
    {    	
        try {
            autoStartTransaction();
            final Edge edge = new SqlEdge(
        		this,
        		(SqlVertex) outVertex,
        		(SqlVertex) inVertex,
        		label);
            autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
            return edge;
        } catch (RuntimeException e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected PreparedStatement getEdgeStatement;
    public Edge getEdge(final Object id) {
    	if (id == null)
    		throw new IllegalArgumentException("SqlGraph.getEdge(id) cannot be null.");
    	
    	try {
    		return new SqlEdge(this, id);
    	} catch (Exception e) {
    		return null;
    	}
    }

    public Iterable<Edge> getEdges() {
        return new SqlEdgeSequence(this);
    }
    
    protected PreparedStatement countEdgesStatement;
    public long countEdges() {
    	try {
			ResultSet rs = countEdgesStatement.executeQuery();
			rs.next();
			long r = rs.getLong(1);
			rs.close();
			return r;
    	}
    	catch (SQLException e) {
    		throw new RuntimeException(e);
    	}
    }
    
    protected PreparedStatement getMaxEdgeIdStatement;
    protected PreparedStatement getEdgeAfterStatement; 
    public Edge getRandomEdge() {
    	try {
    		return SqlEdge.getRandomEdge(this);
    	}
    	catch (SQLException e) {
    		throw new RuntimeException(e);
    	}
    }

    protected PreparedStatement removeEdgeStatement;
    protected PreparedStatement removeEdgePropertiesStatement;
    public void removeEdge(final Edge edge) {
        if (edge == null || edge.getId() == null)
            return;
        
        try {
            autoStartTransaction();
            ((SqlEdge) edge).remove();
            autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    protected PreparedStatement getEdgePropertyStatement;
    protected PreparedStatement getEdgePropertyKeysStatement;
    protected PreparedStatement setEdgePropertyStatement;
    protected PreparedStatement removeEdgePropertyStatement;
    
    public Iterable<Vertex> getShortestPath(final Vertex source, final Vertex target) {
    	return SqlVertexSequence.dijkstra(this, ((SqlVertex) source).vid, ((SqlVertex) target).vid);
    }

    public void clear() {
        try {
        	Statement statement = this.connection.createStatement();
        	statement.executeUpdate("delete from "+SqlGraph.this.namePrefix+"edgeproperty");
        	statement.executeUpdate("delete from "+SqlGraph.this.namePrefix+"vertexproperty");
        	statement.executeUpdate("delete from "+SqlGraph.this.namePrefix+"edge");
        	statement.executeUpdate("delete from "+SqlGraph.this.namePrefix+"vertex");
    		statement.close();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }   
    }
    
    public void delete() {
    	
    	// Commit if necessary
    	
    	if (tx.get().booleanValue()) {
            try {
            	if (!autoCommit) connection.commit();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
            tx.set(false);
        }
    	
    	// Check other connections
    	
    	String addrExt = toString();
		List<SqlGraph> openConnections = connectionsPerAddr.get(addrExt);
		if (openConnections.size() > 1) {
			System.err.println("Warning: SqlGraph.delete() called, but more than one connection is open (might deadlock)");
			System.err.println("Open connections:");
			for (SqlGraph g : openConnections) {
				System.err.println("    " + g.connection);
				for (int i = 1; i < g.constructorStackTrace.length; i++) {
					System.err.println("        at " + g.constructorStackTrace[i]);
				}
			}
		}
		
		// Delete
    				
        try {
        	Statement statement = this.connection.createStatement();
        	statement.executeUpdate("DROP PROCEDURE IF EXISTS "+namePrefix+"dijkstra");
        	statement.executeUpdate("drop table "+SqlGraph.this.namePrefix+"edgeproperty");
        	statement.executeUpdate("drop table "+SqlGraph.this.namePrefix+"vertexproperty");
        	statement.executeUpdate("drop table "+SqlGraph.this.namePrefix+"edge");
        	statement.executeUpdate("drop table "+SqlGraph.this.namePrefix+"vertex");
    		statement.close();
    		close();
    	} catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void shutdown() {
        close();
    }

    public String toString() {
    	return "sqlgraph[" + this.addr
       			+ (this.databaseName.equals("") && this.orgNamePrefix.equals("") ? "" : "|" + this.databaseName)
       			+ (this.orgNamePrefix.equals("") ? "" : "|" + this.orgNamePrefix)
    			+ "]";
    }

    /* TRANSACTIONAL GRAPH INTERFACE */

    protected void autoStartTransaction() {
        if (this.txBuffer.get() > 0) {
            if (!tx.get().booleanValue()) {
                tx.set(true);
                txCounter.set(0);
            }
        }
    }

    public void stopTransaction(final Conclusion conclusion) {
        if (!tx.get().booleanValue()) {
            txCounter.set(0);
            return;
        }

        try {
            if (conclusion == Conclusion.SUCCESS)
            	if (!autoCommit) connection.commit();
            else
            	if (!autoCommit) connection.rollback();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

        tx.set(false);
        txCounter.set(0);
    }

    protected void autoStopTransaction(final Conclusion conclusion) {
        if (this.txBuffer.get() > 0) {
            txCounter.set(txCounter.get() + 1);
            if (conclusion == Conclusion.FAILURE) {
                try {
                	if (!autoCommit) connection.rollback();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
                tx.set(false);
                txCounter.set(0);
            } else if (this.txCounter.get() % this.txBuffer.get() == 0) {
                try {
                	if (!autoCommit) connection.commit();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
                tx.set(false);
                txCounter.set(0);
            }
        }
    }

	@Override
	public int getCurrentBufferSize() {
		return txCounter.get();
	}

	@Override
	public int getMaxBufferSize() {
		return txBuffer.get();
	}

	@Override
	public void setMaxBufferSize(int size) {
        if (tx.get().booleanValue()) {
            try {
				if (!autoCommit) connection.commit();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
            tx.set(false);
        }
        this.txBuffer.set(size);
        this.txCounter.set(0);
	}

	@Override
	public void startBulkLoad() {
		
		bulkLoadMode = true;
		
        try {
        	Statement statement = this.connection.createStatement();
        	statement.executeUpdate("SET unique_checks=0");
        	statement.executeUpdate("SET foreign_key_checks=0");
    		statement.close();
    	} catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
	}

	@Override
	public void stopBulkLoad() {
		
		if (!bulkLoadMode) return;
		
		bulkLoadMode = false;
		
        try {
        	Statement statement = this.connection.createStatement();
        	statement.executeUpdate("SET unique_checks=1");
        	statement.executeUpdate("SET foreign_key_checks=1");
    		statement.close();
    	} catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
	}

	@Override
	public Features getFeatures() {
		return FEATURES;
	}

	@Override
	public Iterable<Vertex> getVertices(String key, Object value) {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<Edge> getEdges(String key, Object value) {
		// TODO
		throw new UnsupportedOperationException();
	}
	
	public boolean isClosed() {
		try {
			return connection.isClosed();
		} catch (SQLException e) {
			return true;
		}
	}
	
	protected void close() {
		
		if (tx.get().booleanValue()) {
            try {
            	if (!autoCommit) connection.commit();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
            tx.set(false);
        }
        		
        try {
        	if (!connection.isClosed()) {
        		
	    		this.connection.close();
	    		
	    		String addrExt = toString();
	    		initSemaphore.acquire();
	    		
	    		try {
		    		numConnectionsPerAddr.put(addrExt, numConnectionsPerAddr.get(addrExt) - 1);
		    		if (!connectionsPerAddr.get(addrExt).remove(this)) {
		    			throw new IllegalStateException("The current instance of SqlGraph is not in connectionsPerAddr");
		    		}
	    		}
	    		finally {
	    			initSemaphore.release();
	    		}
        	}
    	} catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
	}
}
