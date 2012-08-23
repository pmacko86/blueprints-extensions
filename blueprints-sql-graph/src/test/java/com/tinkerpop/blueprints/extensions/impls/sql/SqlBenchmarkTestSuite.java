package com.tinkerpop.blueprints.extensions.impls.sql;

import com.tinkerpop.blueprints.BaseTest;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

/**
 * @author Daniel Margo (http://eecs.harvard.edu/~dmargo)
 */
public class SqlBenchmarkTestSuite extends TestSuite {

    private static final int TOTAL_RUNS = 10;
	
    public SqlBenchmarkTestSuite() {
    }

    public SqlBenchmarkTestSuite(final GraphTest graphTest) {
        super(graphTest);
    }

    public void testSqlGraph() throws Exception {
        double totalTime = 0.0d;
        Graph graph = graphTest.generateGraph();
        GraphMLReader.inputGraph(graph, GraphMLReader.class.getResourceAsStream("graph-example-2.xml"));
        graph.shutdown();

        for (int i = 0; i < TOTAL_RUNS; i++) {
            graph = graphTest.generateGraph();
            this.stopWatch();
            int counter = 0;
            for (final Vertex vertex : graph.getVertices()) {
                counter++;
                for (final Edge edge : vertex.getEdges(Direction.OUT)) {
                    counter++;
                    final Vertex vertex2 = edge.getVertex(Direction.IN);
                    counter++;
                    for (final Edge edge2 : vertex2.getEdges(Direction.OUT)) {
                        counter++;
                        final Vertex vertex3 = edge2.getVertex(Direction.IN);
                        counter++;
                        for (final Edge edge3 : vertex3.getEdges(Direction.OUT)) {
                            counter++;
                            edge3.getVertex(Direction.OUT);
                            counter++;
                        }
                    }
                }
            }
            double currentTime = this.stopWatch();
            totalTime = totalTime + currentTime;
            BaseTest.printPerformance(graph.toString(), counter, "SQL elements touched", currentTime);
            graph.shutdown();
        }
        BaseTest.printPerformance("SQL", 1, "SQL experiment average", totalTime / (double) TOTAL_RUNS);
    }

}

