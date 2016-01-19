package holoma;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;
import org.apache.flink.types.NullValue;

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
	transient private final Graph<String, String, Integer> GRAPH;
	/** Mapping from edge type to weight. */
	private final Map<Integer, Float> MAP_WEIGHT;
	
	
	
	ExecutionEnvironment ENV;
	
	/**
	 * Constructor.
	 * @param depth Depth of the enrichment of connected components.
	 * @param graph Underlying graph.
	 * @param mapWeight Mapping from edge type to weight.
	 */
	public ConnCompEnrichment (int depth, Graph<String, String, Integer> graph,
			Map<Integer, Float> mapWeight, ExecutionEnvironment env) {
		this.DEPTH=depth;
		this.GRAPH=graph;
		this.MAP_WEIGHT=mapWeight;
		this.ENV = env;
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
		Graph<String, NullValue, Integer> subgraph = null;
		Set<String> vertexIds = connComp;
		
		for (int i=1; i<= this.DEPTH; i++) {
			DataSet<Edge<String, Integer>> edges = addNextHop (vertexIds);
			subgraph = Graph.fromDataSet(edges, ENV);
			try {
				vertexIds = new HashSet<String>(subgraph.getVertexIds().collect());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		Graph<String, String, Integer> g = subgraph.mapVertices(new MapperNull2EmptyStr());
		g = g.joinWithVertices(this.GRAPH.getVerticesAsTuple2(), new JoinVertexValue());
		
		
		return g;
	}
	
	
	/**
	 * Adds to the previous subgraph (represented by its vertex IDs)
	 * all vertices (and the corresponding edges) which are one hop away.
	 * @param vertexIds Set of vertices, represent the subgraph.
	 * @return The enriched subgraph (represented by its edges).
	 */
	private DataSet<Edge<String, Integer>> addNextHop (Set<String> vertexIds) {
		final Set<String> targetVertices = vertexIds; 
		DataSet<Edge<String, Integer>> edges = this.GRAPH.getEdges();
		DataSet<Edge<String, Integer>> filteredEdges = edges
				.filter( new FilterFunction<Edge<String, Integer>>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Edge<String, Integer> value) throws Exception {
						return targetVertices.contains(value.getSource());
					}
				});
		return filteredEdges;
	}
	
	
	
	/**
	 * Maps the edge values from edge type to weight according to <code>MAP_WEIGHT</code>.
	 * @param subgraph The graph for which the mapping is executed.
	 * @return A new graph with mapped edges.
	 */
	private Graph<String, String, Float> mapEdgeValues (Graph<String, String, Integer> subgraph) {
		return subgraph.mapEdges(new MapperWeights(this.MAP_WEIGHT));
	}
	
	
	
	
	@SuppressWarnings("serial")
	private final static class JoinVertexValue implements VertexJoinFunction<String, String> {

		@Override
		public String vertexJoin(String vertexValue, String inputValue) throws Exception {
			return inputValue;
		}
		
	}
	
	
	/**
	 * Maps null values to empty strings.
	 */
	@SuppressWarnings("serial")
	private final static class MapperNull2EmptyStr implements MapFunction<Vertex<String, NullValue>, String> {

		@Override
		public String map(Vertex<String, NullValue> value) throws Exception {
			return "";
		}
		
	}
	
	
	/**
	 * Maps edges type to weight.
	 */
	@SuppressWarnings("serial")
	private final static class MapperWeights implements MapFunction<Edge<String, Integer>, Float> {

		
		/** Mapping from edge type to weight. */
		private final Map<Integer, Float> MAP_WEIGHT;
		
		public MapperWeights (Map<Integer, Float> mapWeight) {
			this.MAP_WEIGHT=mapWeight;
		}
		
		@Override
		public Float map(Edge<String, Integer> value) throws Exception {
			return this.MAP_WEIGHT.get(value.f2);
		}
		
	}
	
	

}
