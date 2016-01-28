package ppr;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgeJoinFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.ReduceEdgesFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

public class PersonalizedPageRank {
	
	static Graph<String, VertexValue, Float> enrConnComp;
	
	/** The results. */
	Map<String, List<Vertex<String, VertexValue>>> mapCalcPageRanks;
	
	float teleportProb = 0.5f;
	
	Vertex<String, VertexValue> source = null;
	
	
	/**
	 * Sets the enriched connected component.
	 * @param enrConnComp Enriched connected component.
	 */
	public void setEnrConnComp (Graph<String, VertexValue, Float> enrConnComp) {
		this.enrConnComp=enrConnComp;
	}
	
	/** Initializes the vertex value with 1 iff source, 0 elsewhere.*/
	public void initialize (Vertex<String, VertexValue> v) {
		int vertValue = (v.f0.equals(this.source.f0)) ? 1 : 0;
		
		v.f1.pr=vertValue;
		// determine this.source
	}
	
	
	
	public void startCalculation() {
		//TODO: parameter maxIterations
		Graph<String, VertexValue, Float> calcGraph = 
				this.enrConnComp.runVertexCentricIteration(
						new VertexPageRankUpdater(this.teleportProb, this.source), new PageRankMessenger(), 10);
		
		DataSet<Vertex<String, VertexValue>> resultVector = calcGraph.getVertices();
	}
	
	
	@SuppressWarnings("serial")
	public static final class VertexPageRankUpdater
		extends VertexUpdateFunction<String, VertexValue, Float> {
		
		final float teleportProb;
		final Vertex<String, VertexValue> source;
		
		public VertexPageRankUpdater (float teleportProb, Vertex<String, VertexValue> source) {
			this.teleportProb=teleportProb;
			this.source = source;
		}
		
/*
		DataSet<Vertex<String, Long>> verticesWithComponents = null;		
		
		// #1: Initialize each vertex value with its own and unique component ID
		this.componentGraph = this.GRAPH.mapVertices(
				new MapFunction<Vertex<String, String>, Long>() {
					public Long map (Vertex<String, String> value) {
						// the component ID is the hashCode of the key
						return (long) value.getId().hashCode();
					}
				});	
		
		*/
		
		
		
		//changes 24.1
		
		//for each vertex calculate the total weight of its outgoing edges
		DataSet<Tuple2<String, Float>> sumEdgeWeights = 
				enrConnComp.reduceOnEdges(new SumWeight(), EdgeDirection.OUT);
		
		// assign the transition probabilities as edge weights:
		//divide edge weight by the total weight of outgoing edges for that source 
		Graph<String, VertexValue, Float> networkWithWeights = enrConnComp.joinWithEdgesOnSource(sumEdgeWeights,
						new EdgeJoinFunction<Float, Float>() {

							@Override
							public Float edgeJoin(Float arg0, Float arg1) throws Exception {
								return arg0 / arg1;							}
						});


		
		
		//end changes
		
		@Override
		public void updateVertex(Vertex<String, VertexValue> vertex, MessageIterator<Float> mssgIt) throws Exception {
			
			// sum all of the messages
			float sum = 0;
			for (float msg : mssgIt) 
				sum += msg;
			
			float pr = sum+this.teleportProb*delta(vertex);
			
			VertexValue val = new VertexValue(vertex.getValue().ontName, pr);
			
			setNewVertexValue(val);
			
		}	
		
		private int delta (Vertex<String, VertexValue> v) {
			return (v.f0.equals(this.source.f0)) ? 1 : 0;
		}
	}
	
	
	@SuppressWarnings("serial")
	public static final class PageRankMessenger
		extends MessagingFunction<String, VertexValue, Float, Float> {

		@Override
		public void sendMessages(Vertex<String, VertexValue> arg0) throws Exception {
			for (Edge<String, Float> edge : getEdges()){
				sendMessageTo(edge.getTarget(), arg0.f1.pr + edge.getValue());
			}
			
		}		
	}
	
	@SuppressWarnings("serial")
	static final class SumWeight implements ReduceEdgesFunction<Float> {
			public Float reduceEdges(Float firstEdgeValue, Float secondEdgeValue) {
				return firstEdgeValue+secondEdgeValue;
			}
	}
}

