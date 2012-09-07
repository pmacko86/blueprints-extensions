package com.tinkerpop.blueprints.extensions.impls.sql;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;

import java.lang.reflect.Method;
import java.sql.SQLException;

/**
 * Test suite for BDB graph implementation.
 *
 * @author Daniel Margo (http://eecs.harvard.edu/~dmargo)
 */
public class SqlGraphTest extends GraphTest {
	
	public static SqlGraph graph = null;

    public SqlGraphTest() {
    }

    /*public void testSqlBenchmarkTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new SqlBenchmarkTestSuite(this));
        printTestPerformance("SqlBenchmarkTestSuite", this.stopWatch());
    }*/
    
    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }
    
    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this));
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }
    
    public void testGraphMLReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphMLReaderTestSuite(this));
        printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
    }

    public void testShortestPaths() throws Exception {
        this.stopWatch();
    	doTestSuite(new SqlPathTestSuite(this));
        printTestPerformance("SqlPathTestSuite", this.stopWatch());	
    }
    
    /*public void testTransactionalGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TransactionalGraphTestSuite(this));
        printTestPerformance("TransactionGraphTestSuite", this.stopWatch());
    }

    public void testIndexableGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new IndexableGraphTestSuite(this));
        printTestPerformance("IndexableGraphTestSuite", this.stopWatch());
    }

    public void testIndexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new IndexTestSuite(this));
        printTestPerformance("IndexTestSuite", this.stopWatch());
    }

    public void testAutomaticIndexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new AutomaticIndexTestSuite(this));
        printTestPerformance("AutomaticIndexTestSuite", this.stopWatch());
    }*/

    public static void createDatabase(String name) {
        String db = System.getProperty("sqlGraphAddr");
        if (db == null) db = "//localhost/?user=dmargo&password=kitsune";
    	try {
			SqlGraph.createDatabase(db, name);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
    }

    public static Graph getGraphInstance() {
    	
    	if (graph != null) {
    		if (!graph.isClosed()) return graph;
    	}
    	
        createDatabase("graphdb_test");
    	
        String db = System.getProperty("sqlGraphAddr");
        if (db == null) db = "//localhost/?user=dmargo&password=kitsune";
        graph = new SqlGraph(db, "graphdb_test");
        
        return graph;
    }

    public void doTestSuite(final TestSuite testSuite) throws Exception {
        
        String doTest = System.getProperty("testSqlGraph");
        if (doTest == null || doTest.equals("true")) {
        	
        	((SqlGraph) getGraphInstance()).delete();
            for (Method method : testSuite.getClass().getDeclaredMethods()) {
                if (method.getName().startsWith("test")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                	((SqlGraph) getGraphInstance()).delete();
                }
            }
        }
    }

	@Override
	public Graph generateGraph() {
		// TODO Is this okay?
		return getGraphInstance();
	}
}
