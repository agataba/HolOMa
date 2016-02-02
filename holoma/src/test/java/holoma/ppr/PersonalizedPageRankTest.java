package holoma.ppr;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.junit.Before;
import org.junit.Test;

import holoma.complexDatatypes.EdgeValue;
import holoma.complexDatatypes.VertexValue;

public class PersonalizedPageRankTest {
	
	PersonalizedPageRank ppr = new PersonalizedPageRank();

	@Before
	public void setUp() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();		
		
		List<Vertex<String, VertexValue>> vertexList = new ArrayList<Vertex<String, VertexValue>>();
		vertexList.add(new Vertex<String, VertexValue>("1", new VertexValue("blue", 0f)));
		vertexList.add(new Vertex<String, VertexValue>("3", new VertexValue("blue", 0f)));
		vertexList.add(new Vertex<String, VertexValue>("8", new VertexValue("green", 0f)));
		vertexList.add(new Vertex<String, VertexValue>("9", new VertexValue("green", 0f)));
		vertexList.add(new Vertex<String, VertexValue>("10", new VertexValue("orange", 0f)));
		vertexList.add(new Vertex<String, VertexValue>("11", new VertexValue("orange", 0f)));
		vertexList.add(new Vertex<String, VertexValue>("12", new VertexValue("orange", 0f)));
		vertexList.add(new Vertex<String, VertexValue>("13", new VertexValue("orange", 0f)));
		
		List<Edge<String, EdgeValue>> edgeList = new ArrayList<Edge<String, EdgeValue>>();
		edgeList.add(new Edge<String, EdgeValue>("1","10", new EdgeValue(-1,0.5f)));
		edgeList.add(new Edge<String, EdgeValue>("3","1",new EdgeValue(-1,0.5f)));
		edgeList.add(new Edge<String, EdgeValue>("3","9",new EdgeValue(-1,1f)));
		edgeList.add(new Edge<String, EdgeValue>("9","3",new EdgeValue(-1,1f)));
		edgeList.add(new Edge<String, EdgeValue>("9","8",new EdgeValue(-1,0.5f)));
		edgeList.add(new Edge<String, EdgeValue>("9","11",new EdgeValue(-1,1f)));
		edgeList.add(new Edge<String, EdgeValue>("9","12",new EdgeValue(-1,1f)));
		edgeList.add(new Edge<String, EdgeValue>("11","9",new EdgeValue(-1,1f)));
		edgeList.add(new Edge<String, EdgeValue>("11","10",new EdgeValue(-1,0.5f)));
		edgeList.add(new Edge<String, EdgeValue>("12","9",new EdgeValue(-1,1f)));
		edgeList.add(new Edge<String, EdgeValue>("12","10",new EdgeValue(-1,0.5f)));
		edgeList.add(new Edge<String, EdgeValue>("13","12",new EdgeValue(-1,0.5f)));
		
		Graph<String, VertexValue, EdgeValue> component = Graph.fromCollection(vertexList, edgeList, env);
		
		this.ppr.setEnrConnComp(component);
	}

	@Test
	public void testStart() {
		fail("Not yet implemented");
	}

	@Test
	public void testCalculateOneSource() {
		Vertex<String, VertexValue> source = new Vertex<String, VertexValue>("9", new VertexValue("green", 0f));
		
		try {
			this.ppr.calculateOneSource(source);
			Map<String, List<Vertex<String, VertexValue>>> map = this.ppr.getMapCalcPageRanks();
			for (String src : map.keySet()) {
				System.out.println("source: "+src);
				for (Vertex<String, VertexValue> trg : map.get(src)) {
					System.out.println("  target: "+trg.f0+": "+trg.f1.toString());
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		fail("Not yet implemented");
	}

}
