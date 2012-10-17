package com.tinkerpop.blueprints.extensions.impls.bdb;

import java.io.File;
import java.lang.reflect.Method;

import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;

/**
 * Test suite for BDB graph implementation.
 *
 * @author Daniel Margo (http://eecs.harvard.edu/~dmargo)
 */
public class BdbGraphTest extends GraphTest {

    public BdbGraphTest() {
    }

    /*public void testBdbBenchmarkTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new BdbBenchmarkTestSuite(this));
        printTestPerformance("BdbBenchmarkTestSuite", this.stopWatch());
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

    public void doTestSuite(final TestSuite testSuite) throws Exception {
        String doTest = System.getProperty("testBdbGraph");
        if (doTest == null || doTest.equals("true")) {
            // Need to get 'dupGraphDirectory' or an appropriate default;
            // see other tests for examples.
            String directory = getWorkingDirectory();
            deleteDirectory(new File(directory));
            for (Method method : testSuite.getClass().getDeclaredMethods()) {
                if (method.getName().startsWith("test")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                    // The precise cleanup you'll need to do here is
                    // implementation-dependent; see other tests.
                    deleteDirectory(new File(directory));
                }
            }
        }
    }
    
    @Override
    public Graph generateGraph() {
    	// Here as well, you'll ultimately want to System.getProperty
        return new BdbGraph(getWorkingDirectory());
    }
    
    private String getWorkingDirectory() {
        String directory = System.getProperty("dupGraphDirectory");
        if (directory == null) {
            if (System.getProperty("os.name").toUpperCase().contains("WINDOWS"))
                directory = "C:/temp/blueprints_test";
            else
                directory = "/tmp/blueprints_test";
        }
        return directory;
    }

	//@Override
	public Graph generateGraph(String graphDirectoryName) {
		return new BdbGraph(graphDirectoryName);
	}
}
