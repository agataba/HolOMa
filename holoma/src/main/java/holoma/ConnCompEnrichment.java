package holoma;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;

import holoma.complexDatatypes.VertexValue;

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
	public Graph<String, VertexValue, Float> getEnrichedConnComp (Set<String> connComp) {
		Graph<String, VertexValue, Float> enrConnComp = null;
		
		// calculate the subgraph, i.e. the connected component plus some structure
		Graph<String, VertexValue, Integer> subgraph = extractSubgraph(connComp);
		
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
	private Graph<String, VertexValue, Integer> extractSubgraph (Set<String> connComp) {		
		Graph<String, String, Integer> subgraph = null;
		Set<String> vertexIds = connComp;
		// add vertices:
		// in each step the current vertexIds are expanded by all vertexIds which are one hop away
		for (int i=1; i<= this.DEPTH; i++) {
			try {
				vertexIds = addNextHop (vertexIds);
			} catch (Exception e) { e.printStackTrace(); }
		}
		// create subgraph due to relevant vertices
		final Set<String> relevantVertexIds = vertexIds;
		subgraph = this.GRAPH.filterOnVertices(new FilterFunction<Vertex<String, String>>() {
			public boolean filter(Vertex<String, String> value) throws Exception {
				return relevantVertexIds.contains(value.f0);
			}			
		});
		// change vertex value to complex vertex value type
		Graph<String, VertexValue, Integer> g = subgraph.mapVertices(new MapperNull2VertexVal());			
		
		return g;
	}
	
	
	/**
	 * Adds to the previous subgraph (represented by its vertex IDs)
	 * all vertices (and the corresponding edges) which are one hop away.
	 * @param vertexIds Set of vertices, represent the subgraph.
	 * @return Vertex IDs of the of the current graph plus those which are one hop away.
	 * @throws Exception Converting DataSet to List failed.
	 */
	private Set<String> addNextHop (Set<String> vertexIds) throws Exception {
		final Set<String> currentVertices = vertexIds; 
		Set<String> newVertexIds = new HashSet<String>();
		DataSet<Tuple2<String, String>> edges = this.GRAPH.getEdgeIds();
		
		DataSet<Tuple2<String, String>> filteredEdges = edges
				.filter( new FilterFunction<Tuple2<String, String>>() {
					
					private static final long serialVersionUID = 1L;

					public boolean filter(Tuple2<String, String> value) throws Exception {
						return ( currentVertices.contains(value.f0) 
								|| currentVertices.contains(value.f1));
					}
				});
		
		for (Tuple2<String, String> edge : filteredEdges.collect()) {
			newVertexIds.add(edge.f0);
			newVertexIds.add(edge.f1);
		}
		
		return newVertexIds;		
	}
	
	
	
	/**
	 * Maps the edge values from edge type to weight according to <code>MAP_WEIGHT</code>.
	 * @param subgraph The graph for which the mapping is executed.
	 * @return A new graph with mapped edges.
	 */
	private Graph<String, VertexValue, Float> mapEdgeValues (Graph<String, VertexValue, Integer> subgraph) {
		
		return subgraph.mapEdges(new MapperWeights(this.MAP_WEIGHT));
	}
	
	
	
	/** Adds the ontology name to the vertex value. */
	@SuppressWarnings("serial")
	private final static class JoinVertexValue implements VertexJoinFunction<VertexValue, String> {

		public VertexValue vertexJoin(VertexValue vertexValue, String inputValue) throws Exception {
			vertexValue.ontName=inputValue;
			return vertexValue;
		}		
	}
	
	
	/** Maps simple vertex values to the complex vertex value type. */
	@SuppressWarnings("serial")
	private final static class MapperNull2VertexVal implements MapFunction<Vertex<String, String>, VertexValue> {

		public VertexValue map(Vertex<String, String> value) throws Exception {
			return new VertexValue(value.f1,0f);
		}		
	}
	
	
	/** Maps edge type to weight. */
	@SuppressWarnings("serial")
	private final static class MapperWeights implements MapFunction<Edge<String, Integer>, Float> {
		
		/** Mapping from edge type to weight. */
		private final Map<Integer, Float> MAP_WEIGHT;
		
		public MapperWeights (Map<Integer, Float> mapWeight) {
			this.MAP_WEIGHT=mapWeight;
		}

		public Float map(Edge<String, Integer> value) throws Exception {
			return this.MAP_WEIGHT.get(value.f2);
		}
	}
	
	

}
