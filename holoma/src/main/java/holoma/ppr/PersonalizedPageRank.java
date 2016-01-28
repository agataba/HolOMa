package holoma.ppr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.ReduceEdgesFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

import holoma.HolomaConstants;

public class PersonalizedPageRank {
	
	//static Graph<String, VertexValue, Float> enrConnComp;
	public Graph<String, VertexValue, Float> enrConnComp; // who is right?
	
	/** The results. */
	//source_ID, target_ID, target
	Map<String, List<Vertex<String, VertexValue>>> mapCalcPageRanks = new HashMap<String, List<Vertex<String,VertexValue>>>();
	
	/**
	 * Returns the map of source ID to its pagerank vector.
	 * @return The result.
	 */
	public Map<String, List<Vertex<String, VertexValue>>> getMapCalcPageRanks ()  {
		return this.mapCalcPageRanks;
	}
	
	/**
	 * Starts the computation of pagerank for all sources.
	 * @param enrConnComp
	 * @throws Exception
	 */
	public void start (Graph<String, VertexValue, Float> enrConnComp) throws Exception {		
		this.enrConnComp=enrConnComp;
		
		List<Vertex<String, VertexValue>> sources = this.enrConnComp.getVertices().collect();
		
		// iterate over all vertices as sources
		for (Vertex<String, VertexValue> source : sources) {		
			// #1: initialize component
			this.enrConnComp = this.enrConnComp.mapVertices(new InitMapper(source.f0));
			// #2: calculate pagerank 
			try {
				startCalculation(source);
			} catch (Exception e) {
				System.err.println("Exception during Messaging.");
				e.printStackTrace();
			}
		}
	}
	
	
	
	/** Starts calculation of personalized pagerank for one source.
	 * @param source Source.
	 * @throws Exception Exception during Messaging.
	 */
	private void startCalculation(Vertex<String, VertexValue> source) throws Exception {
		// calculate pagerank for one source and for all vertices
		Graph<String, VertexValue, Float> calcGraph = 
				this.enrConnComp.runVertexCentricIteration(
						new VertexPageRankUpdater(HolomaConstants.TELEPORT_PROB, source),
						new PageRankMessenger(this.enrConnComp), HolomaConstants.MAX_ITER_PPR);	
		// save result vector
		List<Vertex<String, VertexValue>> verticesWithPR = calcGraph.getVertices().collect();
		this.mapCalcPageRanks.put(source.f0, verticesWithPR);		
	}
	
	
	/**
	 * Class for updates on the vertices during iteration.
	 * @author agata
	 *
	 */
	@SuppressWarnings("serial")
	public static final class VertexPageRankUpdater
		extends VertexUpdateFunction<String, VertexValue, Float> {
		
		final float teleportProb;
		final Vertex<String, VertexValue> source;
		
		/**
		 * Constructor.
		 * @param teleportProb Teleportation probability (epsilon).
		 * @param source Source vertex.
		 */
		public VertexPageRankUpdater (float teleportProb, Vertex<String, VertexValue> source) {
			this.teleportProb=teleportProb;
			this.source = source;
		}
		
		/** Update method. */
		@Override
		public void updateVertex(Vertex<String, VertexValue> vertex, MessageIterator<Float> mssgIt) throws Exception {
			
			// sum all of the messages
			float sum = 0;
			for (float msg : mssgIt) 
				sum += msg;
			
			float pr = (1-this.teleportProb)*sum+this.teleportProb*delta(vertex);
			
			VertexValue val = new VertexValue(vertex.getValue().ontName, pr);
			
			setNewVertexValue(val);
			
		}	
		
		private int delta (Vertex<String, VertexValue> v) {
			return (v.f0.equals(this.source.f0)) ? 1 : 0;
		}
	}
	
	
	/**
	 * Class for sending messages during iteration.
	 * @author agata
	 *
	 */
	@SuppressWarnings("serial")
	public static final class PageRankMessenger
		extends MessagingFunction<String, VertexValue, Float, Float> {

		Map<String, Float> sumWeights = new HashMap<String, Float>();
		
		/**
		 * Constructor.
		 * @param enrConnComp
		 * @throws Exception Cannot collect DataSet of sums of weights.
		 */
		public PageRankMessenger (Graph<String, VertexValue, Float> enrConnComp) throws Exception {
			DataSet<Tuple2<String,Float>> sumWeightsDS =enrConnComp.reduceOnEdges(new SumWeight(), EdgeDirection.OUT);
			List<Tuple2<String, Float>> sumWeightsList = sumWeightsDS.collect();			
			for (Tuple2<String, Float> tuple : sumWeightsList)
				this.sumWeights.put(tuple.f0, tuple.f1);			
		}
		
		
		/**
		 * Messaging method.
		 * @param arg0 Source vertex.
		 * @throws Exception
		 */
		@Override
		public void sendMessages(Vertex<String, VertexValue> arg0) throws Exception {
			for (Edge<String, Float> edge : getEdges()){
				sendMessageTo(edge.getTarget(),
						arg0.f1.pr*(edge.getValue()/this.sumWeights.get(edge.getSource())));
			}
			
		}		
	}
	
	/**
	 * Initializes the vertices.
	 * @author agata
	 *
	 */
	@SuppressWarnings("serial")
	static final class InitMapper implements MapFunction<Vertex<String,VertexValue>, VertexValue> {
		String sourceId;
		
		public InitMapper (String sourceId) {
			this.sourceId=sourceId;
		}
		
		@Override
		public VertexValue map(Vertex<String, VertexValue> value) throws Exception {
			value.f1.pr = (value.f0.equals(this.sourceId)) ? 1f : 0f;
			return value.f1;
		}		
	}
	
	
	/** Sums up edge weights. */
	@SuppressWarnings("serial")
	static final class SumWeight implements ReduceEdgesFunction<Float> {
			@Override
			public Float reduceEdges(Float firstEdgeValue, Float secondEdgeValue) {
				return firstEdgeValue+secondEdgeValue;
			}
	}
}

