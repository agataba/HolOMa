package holoma.ppr;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

import holoma.complexDatatypes.EdgeValue;
import holoma.complexDatatypes.VertexValue;

/**
 * Evaluates the results of applying the personalized pagerank algorithm
 * to the enriched connected components.
 * @author max
 *
 */
public class PPREvaluation {
	
	/** The underlying structure, the enriched connected component. */
	private Graph<String, VertexValue, EdgeValue> component = null;
	/** 
	 * The result vectors:
	 * the outer map maps from source to its result vector;
	 * the inner map maps from vertexId to its pagerank value.
	 */
	private Map<String, Map<String, VertexValue>> prVectors = null;
	
	
	/**  Constructor.
	 * @param component The underlying structure, the enriched connected component.
	 * @param prVectors The result vectors.
	 */
	public PPREvaluation (Graph<String, VertexValue, EdgeValue> component, 
			Map<String, List<Vertex<String, VertexValue>>> prVectors) {
		this.component=component;
		Map<String, Map<String, VertexValue>> mOuter = new HashMap<String, Map<String, VertexValue>>();
		for (String s : prVectors.keySet()) {
			Map<String, VertexValue> mInner = new HashMap<String, VertexValue>();
			for (Vertex<String, VertexValue> t : prVectors.get(s))
				mInner.put(t.f0, t.f1);
			mOuter.put(s, mInner);			
		}
		this.prVectors=mOuter;
	}
	
	
	/**
	 * Returns a String which represent the pagerank vector for each vertex.
	 * @return Pagerank vectors.
	 */
	public String getPrVectorsAsString () {
		String res = "prVectors (enrConnComp size: "+this.prVectors.size()+"):\n";
		res += "  <source>=(<target>,[<ontName>,<pagerank>])\n";
		for (String src : this.prVectors.keySet()) {
			Map<String, VertexValue> vect = this.prVectors.get(src);
			for (String trg : vect.keySet())
				res += "  src: "+src+" \t trg: "+trg+" \t "+vect.get(trg)+")\n";
		}
		return res;
	}
	
	/**
	 * Returns for each vertex its best friend(s), i.e., the vertex with the highest pagerank.
	 * @return Best friend(s) for each vertex.
	 */
	public Map<String, Set<Tuple2<String, VertexValue>>> getBestFriends () {
		Map<String, Set<Tuple2<String, VertexValue>>> bestFriends = new HashMap<String, Set<Tuple2<String, VertexValue>>>();
		// iterate over each vertex
		for (String vertexId : this.prVectors.keySet()) {
			// ... and find its best friend (or the set of the (same) best friends)
			Set<Tuple2<String, VertexValue>> bestsOfX = new HashSet<Tuple2<String, VertexValue>>();
			Tuple2<String, VertexValue> bestFriend = new Tuple2<String, VertexValue>("", new VertexValue("",-1f));
			Map<String, VertexValue> prVector = this.prVectors.get(vertexId);
			for (String friendId : prVector.keySet()) {	
				if (prVector.get(friendId).pr > bestFriend.f1.pr) {	
					// there is an even better friend
					bestFriend = new Tuple2<String, VertexValue>(friendId, prVector.get(friendId));
					bestsOfX.clear();
					bestsOfX.add(bestFriend);
				} else if (Math.abs(prVector.get(friendId).pr - bestFriend.f1.pr) < 0.000000001f) {
					// there is a friend as good as the current best friend
					Tuple2<String, VertexValue> furtherBestFriend = new Tuple2<String, VertexValue>(friendId, prVector.get(friendId));
					bestsOfX.add(furtherBestFriend);
				}
			}
			// ... and collect the vertex plus its best friend
			bestFriends.put(vertexId, bestsOfX);
		}		
		
		return bestFriends;
	}
	
	/**
	 * Returns for each vertex its best friend(s), i.e., the vertex with the highest pagerank.
	 * The vertex must be a true friend, i.e., you cannot be befriend with yourself.
	 * @return Best friend(s) for each vertex.
	 */
	public Map<String, Set<Tuple2<String, VertexValue>>> getTrueBestFriends () {
		Map<String, Set<Tuple2<String, VertexValue>>> bestFriends = new HashMap<String, Set<Tuple2<String, VertexValue>>>();
		// iterate over each vertex
		for (String vertexId : this.prVectors.keySet()) {
			// ... and find its best friend (or the set of the (same) best friends)
			Set<Tuple2<String, VertexValue>> bestsOfX = new HashSet<Tuple2<String, VertexValue>>();
			Tuple2<String, VertexValue> bestFriend = new Tuple2<String, VertexValue>("", new VertexValue("",-1f));
			Map<String, VertexValue> prVector = this.prVectors.get(vertexId);
			for (String friendId : prVector.keySet()) {	
				if (prVector.get(friendId).pr > bestFriend.f1.pr) {	
					// there is an even better friend
					if (!vertexId.equals(friendId)) {
						// ... it is not the source vertex
						bestFriend = new Tuple2<String, VertexValue>(friendId, prVector.get(friendId));
						bestsOfX.clear();
						bestsOfX.add(bestFriend);
					}
				} else if (Math.abs(prVector.get(friendId).pr - bestFriend.f1.pr) < 0.000000001f) {
					// there is a friend as good as the current best friend
					if (!vertexId.equals(friendId)) {
						// ... it is not the source vertex
						Tuple2<String, VertexValue> furtherBestFriend = new Tuple2<String, VertexValue>(friendId, prVector.get(friendId));
						bestsOfX.add(furtherBestFriend);
					}
				}
			}
			// ... and collect the vertex plus its best friend
			bestFriends.put(vertexId, bestsOfX);
		}		
		
		return bestFriends;
	}
	
	
	/**
	 * Returns for each vertex its best friend(s), i.e., the vertex with the highest pagerank whereby
	 * that vertex is not part of the set <code>noFriends</code>.
	 * @param noFriends Specifies which friends are actual no friends and have to be ignored as a best friend.
	 * @return Best friend(s) for each vertex.
	 */
	public Map<String, Set<Tuple2<String, VertexValue>>> getBestFriends (Set<String> noFriends) {
		Map<String, Set<Tuple2<String, VertexValue>>> bestFriends = new HashMap<String, Set<Tuple2<String, VertexValue>>>();
		// iterate over each vertex
		for (String vertexId : this.prVectors.keySet()) {
			// ... and find its best friend (or the set of the (same) best friends)
			Set<Tuple2<String, VertexValue>> bestsOfX = new HashSet<Tuple2<String, VertexValue>>();
			Tuple2<String, VertexValue> bestFriend = new Tuple2<String, VertexValue>("", new VertexValue("",-1f));
			Map<String, VertexValue> prVector = this.prVectors.get(vertexId);
			for (String friendId : prVector.keySet()) {
				// exclude as best friends vertices which are specified as method parameter
				if (noFriends.contains(friendId)) continue;
				if (prVector.get(friendId).pr > bestFriend.f1.pr) {	
					// there is an even better friend
					bestFriend = new Tuple2<String, VertexValue>(friendId, prVector.get(friendId));
					bestsOfX.clear();
					bestsOfX.add(bestFriend);
				} else if (Math.abs(prVector.get(friendId).pr - bestFriend.f1.pr) < 0.000000001f) {
					// there is a friend as good as the current best friend
					Tuple2<String, VertexValue> furtherBestFriend = new Tuple2<String, VertexValue>(friendId, prVector.get(friendId));
					bestsOfX.add(furtherBestFriend);
				}
			}
			// ... and collect the vertex plus its best friend
			bestFriends.put(vertexId, bestsOfX);
		}		
		
		return bestFriends;
	}
	
	
	/**
	 * Returns for each vertex its worst friend(s), i.e., the vertex with the lowest pagerank.
	 * @return Worst friend(s) for each vertex.
	 */
	public Map<String, Set<Tuple2<String, VertexValue>>> getWorstFriends () {
		Map<String, Set<Tuple2<String, VertexValue>>> worstFriends = new HashMap<String, Set<Tuple2<String, VertexValue>>>();
		// iterate over each vertex
		for (String vertexId : this.prVectors.keySet()) {
			// ... and find its worst friend (or the set of the (same) worst friends)
			Set<Tuple2<String, VertexValue>> worstsOfX = new HashSet<Tuple2<String, VertexValue>>();
			Tuple2<String, VertexValue> worstFriend = new Tuple2<String, VertexValue>("", new VertexValue("", 1000f));
			Map<String, VertexValue> prVector = this.prVectors.get(vertexId);
			for (String friendId : prVector.keySet()) {	
				if (prVector.get(friendId).pr < worstFriend.f1.pr) {	
					// there is an even better friend
					worstFriend = new Tuple2<String, VertexValue>(friendId, prVector.get(friendId));
					worstsOfX.clear();
					worstsOfX.add(worstFriend);
				} else if (Math.abs(prVector.get(friendId).pr - worstFriend.f1.pr) < 0.000000001f) {
					// there is a friend as good as the current best friend
					Tuple2<String, VertexValue> furtherWorstFriend = new Tuple2<String, VertexValue>(friendId, prVector.get(friendId));
					worstsOfX.add(furtherWorstFriend);
				}
			}
			// ... and collect the vertex plus its best friend
			worstFriends.put(vertexId, worstsOfX);
		}		
		
		return worstFriends;
	}
	
	/**
	 * Calculates the mean of all page rank values depending on the source.
	 * @return
	 */
	public Map<String, Float> getStatistMeans () {
		Map<String, Float> meanStatistic = new HashMap<String, Float>();
		for (String source : this.prVectors.keySet()) {
			Map<String, VertexValue> prVector = this.prVectors.get(source);
			float sum = 0f;
			for (String target : prVector.keySet()) sum += prVector.get(target).pr;
			float mean = sum/prVector.size();
			meanStatistic.put(source, mean);
		}
		return meanStatistic;
	}
	
	
	
	
}
