package holoma.ppr;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
	 * Returns a String which represent the pagerank vector for each vertex.
	 * @return Pagerank vectors.
	 */
	public String showPrVectors () {
		String res = "prVectors (size: "+this.prVectors.size()+"):\n";
		for (String src : this.prVectors.keySet()) {
			Map<String, Float> vect = this.prVectors.get(src);
			for (String trg : vect.keySet())
				res += src+"=("+trg+","+vect.get(trg)+")\n";
		}
		return res;
	}
	
	/**
	 * Returns for each vertex its best friend(s), i.e., the vertex with the highest pagerank.
	 * @return Best friend(s) for each vertex.
	 */
	public Map<String, Set<Tuple2<String,Float>>> getBestFriends () {
		Map<String, Set<Tuple2<String, Float>>> bestFriends = new HashMap<String, Set<Tuple2<String, Float>>>();
		try {
			initCheck();
			// iterate over each vertex
			for (String vertexId : this.prVectors.keySet()) {
				// ... and find its best friend (or the set of the (same) best friends)
				Set<Tuple2<String, Float>> bestsOfX = new HashSet<Tuple2<String, Float>>();
				Tuple2<String, Float> bestFriend = new Tuple2<String, Float>("",-1f);
				Map<String, Float> prVector = this.prVectors.get(vertexId);
				for (String friendId : prVector.keySet()) {	
					if (prVector.get(friendId) > bestFriend.f1) {	
						// there is an even better friend
						bestFriend = new Tuple2<String, Float>(friendId, prVector.get(friendId));
						bestsOfX.clear();
						bestsOfX.add(bestFriend);
					} else if (Math.abs(prVector.get(friendId) - bestFriend.f1) < 0.000000001f) {
						// there is a friend as good as the current best friend
						Tuple2<String, Float> furtherBestFriend = new Tuple2<String, Float>(friendId, prVector.get(friendId));
						bestsOfX.add(furtherBestFriend);
					}
				}
				// ... and collect the vertex plus its best friend
				bestFriends.put(vertexId, bestsOfX);
			}		
		} catch (Exception e) { e.printStackTrace(); }
		
		return bestFriends;
	}
	
	
	/**
	 * Returns for each vertex its best friend(s), i.e., the vertex with the highest pagerank whereby
	 * that vertex is not part of the set <code>noFriends</code>.
	 * @param noFriends Specifies which friends are actual no friends and have to be ignored as a best friend.
	 * @return Best friend(s) for each vertex.
	 */
	public Map<String, Set<Tuple2<String,Float>>> getBestFriends (Set<String> noFriends) {
		Map<String, Set<Tuple2<String, Float>>> bestFriends = new HashMap<String, Set<Tuple2<String, Float>>>();
		try {
			initCheck();
			// iterate over each vertex
			for (String vertexId : this.prVectors.keySet()) {
				// ... and find its best friend (or the set of the (same) best friends)
				Set<Tuple2<String, Float>> bestsOfX = new HashSet<Tuple2<String, Float>>();
				Tuple2<String, Float> bestFriend = new Tuple2<String, Float>("",-1f);
				Map<String, Float> prVector = this.prVectors.get(vertexId);
				for (String friendId : prVector.keySet()) {
					// exclude as best friends vertices which are specified as method parameter
					if (noFriends.contains(friendId)) continue;
					if (prVector.get(friendId) > bestFriend.f1) {	
						// there is an even better friend
						bestFriend = new Tuple2<String, Float>(friendId, prVector.get(friendId));
						bestsOfX.clear();
						bestsOfX.add(bestFriend);
					} else if (Math.abs(prVector.get(friendId) - bestFriend.f1) < 0.000000001f) {
						// there is a friend as good as the current best friend
						Tuple2<String, Float> furtherBestFriend = new Tuple2<String, Float>(friendId, prVector.get(friendId));
						bestsOfX.add(furtherBestFriend);
					}
				}
				// ... and collect the vertex plus its best friend
				bestFriends.put(vertexId, bestsOfX);
			}		
		} catch (Exception e) { e.printStackTrace(); }
		
		return bestFriends;
	}
	
	
	/**
	 * Returns for each vertex its worst friend(s), i.e., the vertex with the lowest pagerank.
	 * @return Worst friend(s) for each vertex.
	 */
	public Map<String, Set<Tuple2<String,Float>>> getWorstFriends () {
		Map<String, Set<Tuple2<String, Float>>> worstFriends = new HashMap<String, Set<Tuple2<String, Float>>>();
		try {
			initCheck();
			// iterate over each vertex
			for (String vertexId : this.prVectors.keySet()) {
				// ... and find its worst friend (or the set of the (same) worst friends)
				Set<Tuple2<String, Float>> worstsOfX = new HashSet<Tuple2<String, Float>>();
				Tuple2<String, Float> worstFriend = new Tuple2<String, Float>("",1000f);
				Map<String, Float> prVector = this.prVectors.get(vertexId);
				for (String friendId : prVector.keySet()) {	
					if (prVector.get(friendId) < worstFriend.f1) {	
						// there is an even better friend
						worstFriend = new Tuple2<String, Float>(friendId, prVector.get(friendId));
						worstsOfX.clear();
						worstsOfX.add(worstFriend);
					} else if (Math.abs(prVector.get(friendId) - worstFriend.f1) < 0.000000001f) {
						// there is a friend as good as the current best friend
						Tuple2<String, Float> furtherWorstFriend = new Tuple2<String, Float>(friendId, prVector.get(friendId));
						worstsOfX.add(furtherWorstFriend);
					}
				}
				// ... and collect the vertex plus its best friend
				worstFriends.put(vertexId, worstsOfX);
			}		
		} catch (Exception e) { e.printStackTrace(); }
		
		return worstFriends;
	}
	
	
	
	/** Checks setting of evaluation data. */
	private void initCheck () throws NullPointerException {
		if (this.component == null || this.prVectors == null)
			throw new NullPointerException("The evaluation data are not set.");
	}

}
