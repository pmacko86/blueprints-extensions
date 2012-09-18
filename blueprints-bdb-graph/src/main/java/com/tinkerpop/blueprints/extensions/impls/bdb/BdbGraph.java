package com.tinkerpop.blueprints.extensions.impls.bdb;

import com.sleepycat.db.BtreeStats;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseStats;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.QueueStats;
import com.sleepycat.db.StatsConfig;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.extensions.BenchmarkableGraph;
import com.tinkerpop.blueprints.extensions.BulkloadableGraph;
import com.tinkerpop.blueprints.extensions.impls.bdb.util.BdbEdgeSequence;
import com.tinkerpop.blueprints.extensions.impls.bdb.util.BdbRecordNumberComparator;
import com.tinkerpop.blueprints.extensions.impls.bdb.util.BdbVertexSequence;

import java.io.File;

/**
 * A Blueprints implementation of Berkeley database using duplicates (http://www.oracle.com)
 *
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 * @author Elaine Angelino (http://www.eecs.harvard.edu/~elaine)
 * @author Peter Macko (http://www.eecs.harvard.edu/~pmacko)
 */
@SuppressWarnings("deprecation")
public class BdbGraph implements Graph, BenchmarkableGraph, BulkloadableGraph {
	
	final protected static BdbRecordNumberComparator recordNumberComparator = new BdbRecordNumberComparator();
	
    private Environment dbEnv;
  
    public Database vertexDb;
    public Database outDb;
    public Database inDb;
    public Database inDbRandom;
    protected Database vertexPropertyDb;
    protected Database edgePropertyDb;
    
    final public DatabaseEntry key = new DatabaseEntry();
    final public DatabaseEntry data = new DatabaseEntry();

    boolean bulkLoadMode = false;
	
	
	// Features

    private static final Features FEATURES = new Features();

    static {

        FEATURES.supportsSerializableObjectProperty = true;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = true;
        FEATURES.supportsUniformListProperty = true;
        FEATURES.supportsMixedListProperty = true;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = true;
        FEATURES.supportsStringProperty = true;

        FEATURES.supportsDuplicateEdges = false;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = true;
        FEATURES.isWrapper = false;
        FEATURES.isRDFModel = false;
        FEATURES.supportsVertexIteration = false;	//TODO
        FEATURES.supportsEdgeIteration = false;		//TODO
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
     * Creates a new instance of a BdbGraph at directory.
     *
     * @param directory The database environment's persistent directory name.
     * @param persistentCacheSize the database cache size (in MB) for the persistent data.
     */
    public BdbGraph(final String directory, int persistentCacheSize) {
        try {
        	File envHome = new File(directory);
        	envHome.mkdirs();
        	
        	EnvironmentConfig envConf = new EnvironmentConfig();
            envConf.setAllowCreate(true);
            envConf.setCacheMax(persistentCacheSize);
            envConf.setInitializeCache(true);
            //envConf.setInitializeLocking(true);
            //envConf.setTransactional(true);
            
            this.dbEnv = new Environment(envHome, envConf);        
     
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setType(DatabaseType.BTREE);
            dbConfig.setSortedDuplicates(true);
            this.outDb = this.dbEnv.openDatabase(null, "out.db", null, dbConfig);
            this.inDb = this.dbEnv.openDatabase(null, "in.db", null, dbConfig);
            this.vertexPropertyDb = this.dbEnv.openDatabase(null, "vertexProperty.db", null, dbConfig);
            this.edgePropertyDb = this.dbEnv.openDatabase(null, "edgeProperty.db", null, dbConfig);
            
            dbConfig.setBtreeComparator(recordNumberComparator);
            dbConfig.setReadOnly(true);
            dbConfig.setAllowCreate(false);
            this.inDbRandom = this.dbEnv.openDatabase(null, "in.db", null, dbConfig);

            dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setSortedDuplicates(false);
            dbConfig.setType(DatabaseType.QUEUE);
            dbConfig.setRecordLength(0);
            this.vertexDb = this.dbEnv.openDatabase(null, "vertex.db", null, dbConfig);
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }        
    }

    
    /**
     * Creates a new instance of a BdbGraph at directory.
     *
     * @param directory The database environment's persistent directory name.
     */
    public BdbGraph(final String directory) {
    	 this(directory, 256);
    }
    

    // BLUEPRINTS GRAPH INTERFACE

    public Vertex addVertex(final Object id) {        
        try {
            //autoStartTransaction();
            final Vertex vertex = new BdbVertex(this);
            //autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
            return vertex;
        } catch (RuntimeException e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }     
    }

    public Vertex getVertex(final Object id) {
    	if (id == null)
    		throw new IllegalArgumentException("BdbGraph.getVertex(id) cannot be null.");
    	
    	try {
    		return new BdbVertex(this, id);
    	} catch (Exception e) {
    		return null;
    	}
    }

    public Iterable<Vertex> getVertices() {
    	return new BdbVertexSequence(this);
    }

    public long countVertices() {    	
    	// Note: The fast version of StatConfig does not give us the results
    	// we need, so this is kind of slow
    	DatabaseStats s;
		try {
			s = vertexDb.getStats(null, StatsConfig.DEFAULT);
			return ((QueueStats) s).getNumData();
		} catch (DatabaseException e) {
			throw new RuntimeException(e);
		}
    }

    public void removeVertex(final Vertex vertex) {
        if (vertex == null || vertex.getId() == null)
            return;
        try {
            //autoStartTransaction();
            ((BdbVertex) vertex).remove();
            //autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    public Vertex getRandomVertex() {
    	try {
    		return BdbVertex.getRandomVertex(this);
    	}
    	catch (DatabaseException e) {
    		throw new RuntimeException(e);
    	}
    }

    public Edge addEdge(
		final Object id,
		final Vertex outVertex,
		final Vertex inVertex,
		final String label)
    {    	
        try {
            //autoStartTransaction();
            final Edge edge = new BdbEdge(
        		this,
        		(BdbVertex) outVertex,
        		(BdbVertex) inVertex,
        		label);
            //autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
            return edge;
        } catch (RuntimeException e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Edge getEdge(final Object id) {
    	if (id == null)
    		throw new IllegalArgumentException("BdbGraph.getEdge(id) cannot be null.");
    	
    	try {
    		return new BdbEdge(this, id);
    	} catch (Exception e) {
    		return null;
    	}
    }

    public Iterable<Edge> getEdges() {
    	return new BdbEdgeSequence(this);
    }

    public long countEdges() {    	
    	// Note: The fast version of StatConfig does not give us the results
    	// we need, so this is kind of slow
    	DatabaseStats s;
		try {
			s = outDb.getStats(null, StatsConfig.DEFAULT);
	    	return ((BtreeStats) s).getNumData();
		} catch (DatabaseException e) {
			throw new RuntimeException(e);
		}
    }

    public void removeEdge(final Edge edge) {
        if (edge == null || edge.getId() == null)
            return;
        try {
            //autoStartTransaction();
            ((BdbEdge) edge).remove();
            //autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    public Edge getRandomEdge() {
    	try {
    		return BdbEdge.getRandomEdge(this);
    	}
    	catch (DatabaseException e) {
    		throw new RuntimeException(e);
    	}
    }

    public void clear() {
        try {
        	vertexDb.truncate(null, false);
            outDb.truncate(null, false);
            inDb.truncate(null, false);
        	vertexPropertyDb.truncate(null,  false);
        	edgePropertyDb.truncate(null, false);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }   
    }

    public void shutdown() {
        try {
            edgePropertyDb.close();
            edgePropertyDb = null;

            vertexPropertyDb.close();
            vertexPropertyDb = null;
            
            inDb.close();
            inDb = null;
            
            outDb.close();
            outDb = null;
            
            vertexDb.close();
            vertexDb = null;
            
            inDbRandom.close();
            inDbRandom = null;
            
            dbEnv.close();
            dbEnv = null;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }   
    }

    public String toString() {
    	try {
    		return "bdbgraph[" + dbEnv.getHome() + "]";
    	} catch (Exception e) {
    		return "bdbgraph[?]";
    	}
    }

	/**
	 * Start bulk load mode
	 */
	@Override
	public void startBulkLoad() {
		bulkLoadMode = true;
	}

	/**
	 * Stop bulk load mode
	 */
	@Override
	public void stopBulkLoad() {
		bulkLoadMode = false;
	}

	@Override
	public Features getFeatures() {
		return FEATURES;
	}

	@Override
	public Iterable<Vertex> getVertices(String key, Object value) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<Edge> getEdges(String key, Object value) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
}
