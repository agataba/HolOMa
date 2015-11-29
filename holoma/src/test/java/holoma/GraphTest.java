package holoma;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.junit.Test;

import junit.framework.Assert;

public class GraphTest {

	@Test
	public void test() {
		fail("Not yet implemented");
	}
	

	
	public void graphTestRunning (){
		GraphCreationPoint graphCreation = new GraphCreationPoint();
		Graph<String, String, Integer> graph = graphCreation.getGraphFromEdgeVertexFile("/src/main/resources/example_edges.csv", "/src/main/resources/example_vertices.csv");
		Assert.assertTrue(!((List<Edge<String, Integer>>) graph).isEmpty());
	}

}
