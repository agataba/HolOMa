package holoma.ppr;

import holoma.HolomaConstants;
import holoma.complexDatatypes.EdgeValue;
import holoma.complexDatatypes.Vertex2RankMap;
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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeJoinFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

public class PersonalizedPageRank implements Serializable{
	
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
	 * @param graph
	 * @return
	 */
	public DataSet<Tuple4<Long,String,String,Float>> calculatePPrForEachCompAndSource (Graph<String,VertexValue,EdgeValue> graph){
		
		/*
		 * consider only the vertices and edges that exist at least in one component
		 */
		Graph<String, VertexValue,EdgeValue> reducedGraph = graph.filterOnVertices(new FilterFunction <Vertex<String,VertexValue>>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = -3693194847465381033L;

			@Override
			public boolean filter(Vertex<String, VertexValue> value)
					throws Exception {
				if (value.getValue().getCompIds().isEmpty()){
					return false;
				}else {
					return true;
				}	
			}
		});
		/*
		 * compute the sum of weights for the outgoing edges
		 */
		OverallSumCalculator sum = new OverallSumCalculator();
		DataSet<Tuple2<String,Map<Long,Float>>> is_aSum = sum.getOverallSumDataSet(false, reducedGraph);
		
		/*
		 * insert the edge weights  
		 */
		reducedGraph = reducedGraph.joinWithEdgesOnSource(is_aSum, new EdgeJoinFunction<EdgeValue,Map<Long,Float>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1768058051277440628L;
			@Override
			public EdgeValue edgeJoin(EdgeValue edgeValue,
					Map<Long, Float> inputValue) throws Exception {
				edgeValue.setComponent2OverallIsA(inputValue);
				return edgeValue;
			}

			
		});
		/*
		 * Initialize the result map by setting the probability for the considered vertex to 1
		 */
		Graph<String,Map<Long,Vertex2RankMap>,EdgeValue> perReducedGraph = reducedGraph.mapVertices(new MapFunction<Vertex<String,VertexValue>,Map<Long,Vertex2RankMap>>(){
		
			/**
			 * 
			 */
			private static final long serialVersionUID = -3231763061833238681L;

			@Override
			public  Map<Long,Vertex2RankMap> map(
					Vertex<String, VertexValue> value) throws Exception {
				Map<Long,Vertex2RankMap> comp2Map = new HashMap<Long,Vertex2RankMap>();
				for (Long id :value.f1.getCompIds()){
					Vertex2RankMap compToRank = new Vertex2RankMap();
					compToRank.addRankForVertex(value.getId(), 1f);
					comp2Map.put(id, compToRank);
				}
				return comp2Map;
			}
			
		});
		
		
		Graph<String,Map<Long,Vertex2RankMap>,EdgeValue> overallPagerankGraph = perReducedGraph.runVertexCentricIteration(new VertexPageRankUpdaterAll(HolomaConstants.TELEPORT_PROB),
				new PageRankMessengerAll(), 10);
		DataSet<Tuple4<Long,String,String,Float>> output = overallPagerankGraph.getVertices().flatMap(
				new FlatMapFunction<Vertex<String,Map<Long,Vertex2RankMap>>,Tuple4<Long,String,String,Float>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(Vertex<String, Map<Long,Vertex2RankMap>> value,
					Collector<Tuple4<Long, String, String, Float>> out)
					throws Exception {
				for (Entry<Long,Vertex2RankMap> e: value.f1.entrySet()){
					for (Entry<String,Float> e2: e.getValue().getRankings().entrySet()){
						Tuple4<Long,String,String,Float> t = new Tuple4<Long,String,String,Float>(e.getKey(),value.f0,e2.getKey(),e2.getValue());
						out.collect(t);
					}
				}
			}
			});
		return output;
	}
	
	
	@SuppressWarnings("serial")
	private class VertexPageRankUpdaterAll
	extends VertexUpdateFunction<String, Map<Long,Vertex2RankMap>, Map<Long,Vertex2RankMap>> {
		private float teleportProb;
		public VertexPageRankUpdaterAll(float teleportProb) {
		this.teleportProb = teleportProb;
	
		}

		
		@Override
		public void updateVertex(Vertex<String, Map<Long,Vertex2RankMap>> vertex,
				MessageIterator<Map<Long, Vertex2RankMap>> inMessages)
				throws Exception {
			Map <Long,Vertex2RankMap> ownMap = vertex.getValue();
			Set<Long> ownCompIds = ownMap.keySet();
			float aggregatedDelta =0;
			Map<Long,Vertex2RankMap> updatedComponentMap = new HashMap<Long,Vertex2RankMap>();
 			
			for (Map<Long,Vertex2RankMap> recCompToRankMap : inMessages){
 				Set<Long> receivedIds = recCompToRankMap.keySet();
 				receivedIds.retainAll(ownCompIds);
 				for (Long includedComponent : receivedIds){
 					Vertex2RankMap sumMap = updatedComponentMap.get(includedComponent);
 					if (sumMap ==null){
 						sumMap =new Vertex2RankMap();
 						updatedComponentMap.put(includedComponent, sumMap);
 					}
					Vertex2RankMap receivedRankingMap = recCompToRankMap.get(includedComponent);
					for (Entry<String,Float> entry: receivedRankingMap.getRankings().entrySet()){
						if (sumMap.getRankings().get(entry.getKey())==null){
							sumMap.addRankForVertex(entry.getKey(), entry.getValue());
						}else{
							sumMap.addRankForVertex(entry.getKey(),
									sumMap.getRankings().get(entry.getKey())+entry.getValue());
						}
					}
				}	
			}
			//Map<Long,Vertex2RankMap> updatedComponentMap2 = new HashMap<Long,Vertex2RankMap>();
			Map<Long,Vertex2RankMap> updatedComponentMap2 = vertex.getValue();
			for (Entry<Long,Vertex2RankMap> e:updatedComponentMap.entrySet()){
				Vertex2RankMap updatedMap =updatedComponentMap2.get(e.getKey());
				updatedComponentMap2.put(e.getKey(), updatedMap);
				 Vertex2RankMap oldMap = vertex.getValue().get(e.getKey());
				for (Entry<String,Float> entry: e.getValue().getRankings().entrySet()){
					float pr = (1-this.teleportProb)*entry.getValue();
					
					Float oldppr ;
					if (oldMap ==null && !entry.getKey().equals(vertex.f0)){
						oldppr = 0f;
						aggregatedDelta += Math.abs(pr-oldppr);		
					}else if (!entry.getKey().equals(vertex.f0)&&oldMap !=null){
						oldppr = oldMap.getRankings().get(entry.getKey());
						if (oldppr ==null){
							oldppr =0f;
						}
						aggregatedDelta += Math.abs(pr-oldppr);		
					}	
					updatedMap.addRankForVertex(entry.getKey(), pr);
				}
			}
			
			for (Entry<Long,Vertex2RankMap> e:updatedComponentMap2.entrySet()){
				 Vertex2RankMap oldMap = vertex.getValue().
							get(e.getKey());
				Vertex2RankMap updatedMap = e.getValue();
				float pr =0;
				if (updatedMap.getRankings().containsKey(vertex.f0)){
					 pr = updatedMap.getRankings().get(vertex.f0)+
							this.teleportProb*1f;
					updatedMap.addRankForVertex(vertex.f0,pr);
				}else {
					pr = this.teleportProb*1f;
				}
				float oldppr =oldMap.getRankings().get(vertex.f0);
				aggregatedDelta += Math.abs(pr-oldppr);	
				updatedMap.addRankForVertex(vertex.f0,pr);
			}
 			if (aggregatedDelta>0.001){
 				setNewVertexValue(updatedComponentMap2);
 			}else {
 				log.info("deactivate node "+aggregatedDelta);
 			}
		}
	}	
	
	
	
	@SuppressWarnings("serial")
	private class PageRankMessengerAll extends MessagingFunction<String, Map<Long,Vertex2RankMap>, Map<Long,Vertex2RankMap>, EdgeValue>{		
		public PageRankMessengerAll (){
		
		}
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void sendMessages(Vertex<String, Map<Long,Vertex2RankMap>> vertex)
				throws Exception {
			
			for (Edge<String, EdgeValue> edge : getEdges()){
				
				float value = 0;
				Map<Long,Vertex2RankMap> prMapPerVertex = new HashMap<Long,Vertex2RankMap>();
				for (Entry<Long,Vertex2RankMap> w: vertex.f1.entrySet()){
					Vertex2RankMap sendingMap = new Vertex2RankMap ();
					for (Entry<String,Float> rankPerVertex : w.getValue().getRankings().entrySet()){
						float nominator = edge.f2.getComponent2OverallIsA().get(w.getKey());
						value = rankPerVertex.getValue()*(edge.getValue().weight/nominator);
						sendingMap.addRankForVertex(rankPerVertex.getKey(), value);
					}
					prMapPerVertex.put(w.getKey(), sendingMap);
				}
				sendMessageTo(edge.getTarget(),prMapPerVertex);
				
				
//				if (vertex.f1.getOntName().equals(edge.getValue().getOnt_type())&&
//						edge.getValue().getType() ==1){
//					Map<Long,Vertex2RankMap> prMapPerVertex = new HashMap<Long,Vertex2RankMap>();
//					for (Entry<Long,Vertex2RankMap> w: vertex.f1.getComponentToRank().entrySet()){
//						Vertex2RankMap sendingMap = new Vertex2RankMap ();
//						for (Entry<String,Float> rankPerVertex : w.getValue().getRankings().entrySet()){
//							float nominator = edge.f2.getComponent2OverallInverse().get(w.getKey());
//							value = rankPerVertex.getValue()*(edge.getValue().weight/nominator);
//							if (edge.getValue().weight>nominator)
//								log.warn(edge.toString());
//							sendingMap.addRankForVertex(rankPerVertex.getKey(), value);
//						}
//						prMapPerVertex.put(w.getKey(), sendingMap);
//					}
//					sendMessageTo(edge.getTarget(),prMapPerVertex);
//				}else if (!vertex.f1.getOntName().equals(edge.getValue().getOnt_type())&&
//						edge.getValue().getType() ==2){
//					Map<Long,Vertex2RankMap> prMapPerVertex = new HashMap<Long,Vertex2RankMap>();
//					for (Entry<Long,Vertex2RankMap> w: vertex.f1.getComponentToRank().entrySet()){
//						Vertex2RankMap sendingMap = new Vertex2RankMap ();
//						for (Entry<String,Float> rankPerVertex : w.getValue().getRankings().entrySet()){
//							value = rankPerVertex.getValue()*(edge.getValue().weight/edge.f2.getOverall_inverse_weight());
//							if (edge.getValue().weight>edge.f2.getOverall_inverse_weight())
//								log.warn(edge.toString());
//							sendingMap.addRankForVertex(rankPerVertex.getKey(), value);
//						}
//						prMapPerVertex.put(w.getKey(), sendingMap);
//					}
//					sendMessageTo(edge.getTarget(),prMapPerVertex);
//				}else if (edge.getValue().getType() ==0){
//					 if (!vertex.f1.getOntName().equals(edge.getValue().getOnt_type())){
//						Map<Long,Vertex2RankMap> prMapPerVertex = new HashMap<Long,Vertex2RankMap>();
//						for (Entry<Long,Vertex2RankMap> w: vertex.f1.getComponentToRank().entrySet()){
//							Vertex2RankMap sendingMap = new Vertex2RankMap ();
//							for (Entry<String,Float> rankPerVertex : w.getValue().getRankings().entrySet()){
//								value = rankPerVertex.getValue()*(edge.getValue().weight/edge.f2.getOverall_isA_weight());
//								if (edge.getValue().weight>edge.f2.getOverall_isA_weight())
//									log.warn(edge.toString());
//								sendingMap.addRankForVertex(rankPerVertex.getKey(), value);
//							}
//							prMapPerVertex.put(w.getKey(), sendingMap);
//						}
//						sendMessageTo(edge.getTarget(),prMapPerVertex);
//					}	
//				}
			}	
		}
	}
}

