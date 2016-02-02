package holoma;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.junit.Before;
import org.junit.Test;

import holoma.complexDatatypes.EdgeValue;
import holoma.complexDatatypes.VertexValue;
import holoma.ppr.PPREvaluation;

public class PageRankEvaluationTest {

	PPREvaluation prEval;
	
	@Before
	public void setUp() {		
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
		
		Map<String, List<Vertex<String, VertexValue>>> prVectors = new HashMap<String, List<Vertex<String, VertexValue>>>();
		List<Vertex<String, VertexValue>> innerVector = new ArrayList<Vertex<String, VertexValue>>();
		innerVector.add(new Vertex<String, VertexValue>("1", new VertexValue("blue",1f)));
		innerVector.add(new Vertex<String, VertexValue>("3", new VertexValue("blue",0.6f)));
		innerVector.add(new Vertex<String, VertexValue>("8", new VertexValue("green",0.6f)));
		innerVector.add(new Vertex<String, VertexValue>("9", new VertexValue("green",0.7f)));
		innerVector.add(new Vertex<String, VertexValue>("10", new VertexValue("orange",0.65f)));
		innerVector.add(new Vertex<String, VertexValue>("11", new VertexValue("orange",0.8f)));
		innerVector.add(new Vertex<String, VertexValue>("12", new VertexValue("orange",0.2f)));
		innerVector.add(new Vertex<String, VertexValue>("13", new VertexValue("orange",0.4f)));
		prVectors.put("1", innerVector);
		innerVector = new ArrayList<Vertex<String, VertexValue>>();
		innerVector.add(new Vertex<String, VertexValue>("1",  new VertexValue("blue",0f)));
		innerVector.add(new Vertex<String, VertexValue>("3", new VertexValue("blue",0.6f)));
		innerVector.add(new Vertex<String, VertexValue>("8", new VertexValue("green",0.6f)));
		innerVector.add(new Vertex<String, VertexValue>("9", new VertexValue("green",0.7f)));
		innerVector.add(new Vertex<String, VertexValue>("10", new VertexValue("orange",0.65f)));
		innerVector.add(new Vertex<String, VertexValue>("11", new VertexValue("orange",0.8f)));
		innerVector.add(new Vertex<String, VertexValue>("12", new VertexValue("orange",0.2f)));
		innerVector.add(new Vertex<String, VertexValue>("13", new VertexValue("orange",0.4f)));
		prVectors.put("3", innerVector);
		innerVector = new ArrayList<Vertex<String, VertexValue>>();
		innerVector.add(new Vertex<String, VertexValue>("1",  new VertexValue("blue",0f)));
		innerVector.add(new Vertex<String, VertexValue>("3", new VertexValue("blue",0.6f)));
		innerVector.add(new Vertex<String, VertexValue>("8", new VertexValue("green",0.6f)));
		innerVector.add(new Vertex<String, VertexValue>("9", new VertexValue("green",0.7f)));
		innerVector.add(new Vertex<String, VertexValue>("10", new VertexValue("orange",0.65f)));
		innerVector.add(new Vertex<String, VertexValue>("11", new VertexValue("orange",0.8f)));
		innerVector.add(new Vertex<String, VertexValue>("12", new VertexValue("orange",0f)));
		innerVector.add(new Vertex<String, VertexValue>("13", new VertexValue("orange",0.8f)));
		prVectors.put("8", innerVector);
		
		prEval = new PPREvaluation(component, prVectors);
		
	}

	@Test
	public void testGetBestFriends() {
		Map<String, Set<Tuple2<String,VertexValue>>> expectedResult = new HashMap<String, Set<Tuple2<String,VertexValue>>>();
		Set<Tuple2<String,VertexValue>> bestsOfX = new HashSet<Tuple2<String,VertexValue>>();
		
		Tuple2<String,VertexValue> bestFriend = new Tuple2<String,VertexValue>("1",new VertexValue("blue",1f));
		bestsOfX.add(bestFriend);
		expectedResult.put("1", bestsOfX);
		
		bestsOfX = new HashSet<Tuple2<String,VertexValue>>();
		bestFriend = new Tuple2<String, VertexValue>("11",new VertexValue("orange",0.8f));
		bestsOfX.add(bestFriend);
		expectedResult.put("3", bestsOfX);
		
		bestsOfX = new HashSet<Tuple2<String,VertexValue>>();
		bestFriend = new Tuple2<String, VertexValue>("11",new VertexValue("orange",0.8f));
		bestsOfX.add(bestFriend);
		bestFriend = new Tuple2<String, VertexValue>("13",new VertexValue("orange",0.8f));
		bestsOfX.add(bestFriend);
		expectedResult.put("8", bestsOfX);		
		
		assertEquals(expectedResult, prEval.getBestFriends());
	}
	
	@Test
	public void testGetBestFriendsWithParm() {
		Map<String, Set<Tuple2<String,Float>>> expectedResult = new HashMap<String, Set<Tuple2<String,Float>>>();
		Set<Tuple2<String,Float>> bestsOfX = new HashSet<Tuple2<String,Float>>();
		
		Tuple2<String,Float> bestFriend = new Tuple2<String, Float>("1",1f);
		bestsOfX.add(bestFriend);
		expectedResult.put("1", bestsOfX);
		
		bestsOfX = new HashSet<Tuple2<String,Float>>();
		bestFriend = new Tuple2<String, Float>("9",0.7f);
		bestsOfX.add(bestFriend);
		expectedResult.put("3", bestsOfX);
		
		bestsOfX = new HashSet<Tuple2<String,Float>>();
		bestFriend = new Tuple2<String, Float>("13",0.8f);
		bestsOfX.add(bestFriend);
		expectedResult.put("8", bestsOfX);		
		
		Set<String> noFriends = new HashSet<String>();
		noFriends.add("11");
		assertEquals(expectedResult, prEval.getBestFriends(noFriends));
	}
	
	
	@Test
	public void testGetWorstFriends() {
		Map<String, Set<Tuple2<String,Float>>> expectedResult = new HashMap<String, Set<Tuple2<String,Float>>>();
		Set<Tuple2<String,Float>> worstsOfX = new HashSet<Tuple2<String,Float>>();
		
		Tuple2<String,Float> worstFriend = new Tuple2<String, Float>("12",0.2f);
		worstsOfX.add(worstFriend);
		expectedResult.put("1", worstsOfX);
		
		worstsOfX = new HashSet<Tuple2<String,Float>>();
		worstFriend = new Tuple2<String, Float>("1",0f);
		worstsOfX.add(worstFriend);
		expectedResult.put("3", worstsOfX);
		
		worstsOfX = new HashSet<Tuple2<String,Float>>();
		worstFriend = new Tuple2<String, Float>("1",0f);
		worstsOfX.add(worstFriend);
		worstFriend = new Tuple2<String, Float>("12",0f);
		worstsOfX.add(worstFriend);
		expectedResult.put("8", worstsOfX);		
		
		assertEquals(expectedResult, prEval.getWorstFriends());
	}

}
