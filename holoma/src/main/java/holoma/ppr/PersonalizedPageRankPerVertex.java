package holoma.ppr;

import holoma.HolomaConstants;
import holoma.complexDatatypes.VertexValue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeJoinFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

public class PersonalizedPageRankPerVertex implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8364173519091119286L;
	static Logger log = Logger.getLogger(PersonalizedPageRank.class);	
	/**
	 * calculates for each vertex a vector of pageranks with respect to the vertices that are in the same component
	 * The structure of the computed result per vertex is structured as follows:
	 * Map<Long,Vertex2RangeMap> store the vector of rankings w.r.t the different components of the considered vertex
	 * Vertex2RangeMap := Map <String,Float> store the probability of presence for the considered vertex 
	 * by starting from the vertex that is the key in the map 
	 * @param sumReducedGraph
	 * @return
	 */
	public DataSet<Tuple3<Long, Long, Float>> calculatePPrForEachCompAndSource (Graph<Long, VertexValue, Map<Long, Float>> sumReducedGraph,final Long consideredVertex){
		
		
		/*
		 * compute the sum of weights for the outgoing edges
		 */
		
	
//		DataSet<Tuple2<Long,Float>> is_aSum = reducedGraph.reduceOnEdges(new ReduceEdgesFunction<Float> (){
//		
//			@Override
//			public Float reduceEdges(Float value1, Float value2) {
//				// TODO Auto-generated method stub
//				return value1+value2;
//			}
//			
//		}, EdgeDirection.OUT);
		/*
		 * insert the edge weights  
		 */
	
		
//		reducedGraph = reducedGraph.joinWithEdgesOnSource(is_aSum, new EdgeJoinFunction<Float,Float>(){
//
//			@Override
//			public Float edgeJoin(Float edgeValue, Float inputValue)
//					throws Exception {
//				// TODO Auto-generated method stub
//				return edgeValue/inputValue;
//			}
//		});
		/*
		 * Initialize the result map by setting the probability for the considered vertex to 1
		 */
		Graph<Long,Map<Long,Float>,Map<Long,Float>> perReducedGraph = sumReducedGraph.mapVertices(new MapFunction<Vertex<Long,VertexValue>,Map<Long,Float>>(){
		
			/**
			 * 
			 */
			
			private static final long serialVersionUID = -3231763061833238681L;

			@Override
			public  Map<Long,Float> map(
					Vertex<Long, VertexValue> value) throws Exception {
				if (consideredVertex == value.f0){
					Map<Long,Float> comp2Map = new HashMap<Long,Float>();
					for (Long id :value.f1.getCompIds()){
						
						comp2Map.put(id, 1f);
					}
					return comp2Map;
				}else return new HashMap<Long,Float>();
			}
			
		});
		
		
		Graph<Long,Map<Long,Float>,Map<Long,Float>> overallPagerankGraph = perReducedGraph.runVertexCentricIteration(
				new VertexPageRankUpdater(HolomaConstants.TELEPORT_PROB,consideredVertex),
				new PageRankMessenger(), HolomaConstants.MAX_ITER_PPR);
		DataSet<Tuple3<Long,Long,Float>> output = overallPagerankGraph.getVertices().flatMap(
				new FlatMapFunction<Vertex<Long,Map<Long,Float>>,Tuple3<Long,Long,Float>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(Vertex<Long, Map<Long,Float>> value,
					Collector<Tuple3<Long,Long,Float>> out)
					throws Exception {
				for (Entry<Long,Float> e: value.f1.entrySet()){
					Tuple3<Long,Long,Float> t = new Tuple3<Long,Long,Float>(e.getKey(),value.f0,e.getValue());
					out.collect(t);
					
				}
			}
			});
		return output;
	}
	
	
	
	
	
	@SuppressWarnings("serial")
	private class VertexPageRankUpdater
	extends VertexUpdateFunction<Long, Map<Long,Float>, Map<Long,Float>> {
		private float teleportProb;
		private final Long consideredVertex;
		public VertexPageRankUpdater(float teleportProb,Long consideredVertex2) {
			this.teleportProb = teleportProb;
			consideredVertex = consideredVertex2 ;
		}

		
		@Override
		public void updateVertex(Vertex<Long, Map<Long,Float>> vertex,
				MessageIterator<Map<Long,Float>> inMessages)
				throws Exception {
			Map <Long,Float> ownMap = vertex.getValue();
			Set<Long> ownCompIds = ownMap.keySet();
			float aggregatedDelta =0;
			Map<Long,Float> updatedComponentMap = new HashMap<Long,Float>();
 			
			for (Map<Long,Float> recCompToRankMap : inMessages){
 				Set<Long> receivedIds = recCompToRankMap.keySet();
 				receivedIds.retainAll(ownCompIds);
 				for (Long includedComponent : receivedIds){
 					Float sum = updatedComponentMap.get(includedComponent);
 					if (sum ==null){
 						sum =0f;	
 					}
 					updatedComponentMap.put(includedComponent,
 							recCompToRankMap.get(includedComponent)+sum);
				}	
			}
			Map<Long,Float> updatedComponentMap2 = new HashMap<Long,Float>();
			//Map<Long,Vertex2RankMap> updatedComponentMap2 = vertex.getValue();
			for (Entry<Long,Float> e:updatedComponentMap.entrySet()){
				
				Float oldppr = vertex.getValue().get(e.getKey());
				if (oldppr ==null){
					oldppr =0f;
				}
				float tele = 0;
				if (this.consideredVertex == vertex.f0){
					tele = teleportProb *1f;
				}
				float pr = (1-this.teleportProb)*e.getValue()+tele;	
				aggregatedDelta += Math.abs(pr-oldppr);
				updatedComponentMap2.put(e.getKey(),pr);
			}
			
 			if (aggregatedDelta>0.001){
 				setNewVertexValue(updatedComponentMap2);
 			}else {
 			}
		}
	}	
	
	
	
	@SuppressWarnings("serial")
	private class PageRankMessenger extends MessagingFunction<Long, Map<Long,Float>, Map<Long,Float>, Map<Long,Float>>{		
		public PageRankMessenger (){
		
		}
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void sendMessages(Vertex<Long, Map<Long,Float>> vertex)
				throws Exception {
			
			for (Edge<Long, Map<Long,Float>> edge : getEdges()){
				
				float value = 0;
				Map<Long,Float> prMapPerVertex = new HashMap<Long,Float>();	
				for (Entry<Long,Float> rankPerVertex : vertex.f1.entrySet()){
					if (edge.f2.get(rankPerVertex.getKey())!=null){
						float nominator = edge.f2.get(rankPerVertex.getKey());
						value = rankPerVertex.getValue()*nominator;
						prMapPerVertex.put(rankPerVertex.getKey(), value);
					}
				}
				if(!prMapPerVertex.isEmpty())
					sendMessageTo(edge.getTarget(),prMapPerVertex);
			}	
		}
	}
}
