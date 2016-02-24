package holoma.connComp;

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
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.log4j.Logger;

import com.esotericsoftware.minlog.Log;

import holoma.complexDatatypes.EdgeValue;
import holoma.complexDatatypes.VertexValue;

/**
 * Enriches connected components
 * with additional structure from the given ontologies.
 * @author max
 *
 */
public class ConnCompEnrichment {
	Logger log = Logger.getLogger(getClass());
	/** Depth of the enrichment. */
	private final int DEPTH;
	/** Underlying graph structure. */
	private final Graph<String, VertexValue, EdgeValue> GRAPH;
	/** Mapping from edge type to weight. */
	private final Map<Integer, Float> MAP_WEIGHT;
	
		
	ExecutionEnvironment ENV;
	
	/**
	 * Constructor.
	 * @param depth Depth of the enrichment of connected components.
	 * @param graph2 Underlying graph.
	 * @param mapWeight Mapping from edge type to weight.
	 */
	public ConnCompEnrichment (int depth, Graph<String, VertexValue, EdgeValue> graph2,
			Map<Integer, Float> mapWeight, ExecutionEnvironment env) {
		this.DEPTH=depth;
		this.GRAPH=graph2;
		this.MAP_WEIGHT=mapWeight;
		this.ENV = env;
	}
	
	

	
	public ConnCompEnrichment() {
		this.MAP_WEIGHT=null;
		this.DEPTH=0;
		this.GRAPH=null;
		this.ENV = null;
	}




	public Graph<String,VertexValue,EdgeValue> enrichConnectedComponent (int depth,Graph<String, VertexValue, EdgeValue> graph2,
			ExecutionEnvironment env){
		
		Graph<String,VertexValue,EdgeValue> newGraph =graph2.runVertexCentricIteration(new ComponentIdSetter(), new ComponentIdSender(), depth);
		return newGraph;
	}

	

	/**
	 * Maps the edge values from edge type to weight according to <code>MAP_WEIGHT</code>.
	 * @param subgraph The graph for which the mapping is executed.
	 * @return A new graph with mapped edges.
	 */
//	private Graph<String, VertexValue, EdgeValue> mapEdgeValues (Graph<String, VertexValue, EdgeValue> subgraph) {
//		
//		return subgraph.mapEdges(new MapperWeights(this.MAP_WEIGHT));
//	}
	

	
	
	/**
	 * a vertex updates its set of component id by the received ids
	 * @author christen
	 *
	 */
	@SuppressWarnings("serial")
	public static final class  ComponentIdSetter extends 
	VertexUpdateFunction<String,VertexValue,Set<Long>>{

		/**
		 * 
		 */
		private static final long serialVersionUID = -2113735965221491842L;

		@Override
		public void updateVertex(Vertex<String, VertexValue> vertex,
				MessageIterator<Set<Long>> inMessages) throws Exception {
			VertexValue vv = vertex.getValue();
			Set<Long> comId =vv.getCompIds();
			for (Set<Long> newCompId: inMessages){
				comId.addAll(newCompId);
			}
			this.setNewVertexValue(vv);
		}
		
	}
	
	
	/**
	 * a vertex sends its ids to the neighbor vertices
	 * @author christen
	 *
	 */
	@SuppressWarnings("serial")
	public static final class ComponentIdSender extends MessagingFunction<String,VertexValue,Set<Long>,EdgeValue>{

		/**
		 * 
		 */
		private static final long serialVersionUID = -7593914175037009555L;

		@Override
		public void sendMessages(Vertex<String, VertexValue> vertex)
				throws Exception {
			if (!vertex.getValue().getCompIds().isEmpty()){
				for (Edge<String,EdgeValue>e:this.getEdges()){
					sendMessageTo(e.getTarget(), vertex.getValue().getCompIds());
				}	
			}	
		}	
	}
}
