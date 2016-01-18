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
		
		return enrConnComp;
	}
	

}
