package holoma;

import java.util.Map;
import java.util.Set;

import org.apache.flink.graph.Graph;

/**
 * Enriches connected components
 * with additional structure from the given ontologies.
 * @author max
 *
 */
public class ConnCompEnrichment {
	
	/** Depth of the enrichment. */
	private final int DEPTH;
	/** Underlying graph structure. */
	private final Graph<String, String, Integer> GRAPH;
	/** Mapping from edge type to weight. */
	private final Map<Integer, Float> MAP_WEIGHT;
	
	
	/**
	 * Constructor.
	 * @param depth Depth of the enrichment of connected components.
	 * @param graph Underlying graph.
	 * @param mapWeight Mapping from edge type to weight.
	 */
	public ConnCompEnrichment (int depth, Graph<String, String, Integer> graph,
			Map<Integer, Float> mapWeight) {
		this.DEPTH=depth;
		this.GRAPH=graph;
		this.MAP_WEIGHT=mapWeight;
	}
	
	
	/**
	 * Starts the enrichment of the connected component and
	 * returns the result as a graph.
	 * @param connComp A set of vertices which are a connected component.
	 * @return The enriched connected component.
	 */
	public Graph<String, String, Float> getEnrichedConnComp (Set<String> connComp) {
		Graph<String, String, Float> enrConnComp = null;
		
		// calculate the subgraph, i.e. the connected component plus some structure
		Graph<String, String, Integer> subgraph = extractSubgraph(connComp);
		
		// map the values of the edges from type to weight
		enrConnComp = mapEdgeValues (subgraph);
		
		return enrConnComp;
	}
	
	
	/**
	 * Extracts a subgraph from the given graph. The subgraph contains the connected
	 * component plus structure according the depth <code>DEPTH</code>.
	 * @param connComp A connected component within <code>GRAPH</code>.
	 * @return The subgraph around the connected component.
	 */
	private Graph<String, String, Integer> extractSubgraph (Set<String> connComp) {
		return null;
	}
	
	
	/**
	 * Maps the edge values from edge type to weight according to <code>MAP_WEIGHT</code>.
	 * @param g The graph for which the mapping is executed.
	 * @return A new graph with mapped edges.
	 */
	private Graph<String, String, Float> mapEdgeValues (Graph<String, String, Integer> g) {
		return null;
	}
	

}
