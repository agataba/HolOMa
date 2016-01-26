package holoma.ppr;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;

import holoma.complexDatatypes.VertexValue;

/**
 * Evaluates the results of applying the personalized pagerank algorithm
 * to the enriched connected components.
 * @author max
 *
 */
public class PageRankEvaluation {
	
	/** The underlying structure, the enriched connected component. */
	private Graph<String, VertexValue, Float> component = null;
	/** 
	 * The result vectors:
	 * the outer map maps from source to its result vector;
	 * the inner map maps from vertexId to its pagerank value.
	 */
	private Map<String, Map<String, Float>> prVectors = null;
	
	
	/**  Constructor. */
	public PageRankEvaluation () { }
	
	
	/**
	 * Sets the data that are necessary for evaluation.
	 * @param component The underlying structure, the enriched connected component.
	 * @param prVectors The result vectors.
	 */
	public void setEvalData (Graph<String, VertexValue, Float> component, Map<String, Map<String, Float>> prVectors) {
		this.component=component;
		this.prVectors=prVectors;
	}
	
	/**
	 * Returns for each vertex its best friend, i.e., the vertex with the highest pagerank.
	 * @return Best friend for each vertex.
	 */
	public Map<String, Tuple2<String,Float>> getBestFriends () {
		Map<String, Tuple2<String, Float>> bestFriends = new HashMap<String, Tuple2<String, Float>>();
		try {
			initCheck();
			// iterate over each vertex
			for (String vertexId : this.prVectors.keySet()) {
				// ... and find its best friend
				Tuple2<String, Float> bestFriend = new Tuple2<String, Float>("",-1f);
				Map<String, Float> prVector = this.prVectors.get(vertexId);
				for (String friendId : prVector.keySet()) {
					if (prVector.get(friendId) > bestFriend.f1) {
						bestFriend.f0 = friendId;
						bestFriend.f1 = prVector.get(friendId);
					}
				}
				// ... and collect the vertex plus its best friend
				bestFriends.put(vertexId, bestFriend);
			}		
		} catch (Exception e) { e.printStackTrace(); }
		
		return bestFriends;
	}
	
	
	
	/** Checks setting of evaluation data. */
	private void initCheck () throws NullPointerException {
		if (this.component == null || this.prVectors == null)
			throw new NullPointerException("The evaluation data are not set.");
	}

}
