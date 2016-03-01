package holoma.connComp;

import java.io.Serializable;

import holoma.complexDatatypes.VertexValue;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

public class ConnectedComponentSizeFilter implements Serializable{

	static Logger log  = Logger.getLogger(ConnectedComponentSizeFilter.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static Graph <Long,VertexValue,Float> filterGraphByComponentSize(Graph<Long,VertexValue,Float> graph2,
			final float minSize, final float maxSize){
		DataSet<Tuple2<Long,Integer>> compStat = graph2.getVertices().flatMap(new FlatMapFunction<Vertex<Long,VertexValue>,
				Tuple2<Long,Integer>>(){

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void flatMap(Vertex<Long, VertexValue> value,
							Collector<Tuple2<Long, Integer>> out)
							throws Exception {
						for (Long cid : value.getValue().getCompIds()){
							out.collect(new Tuple2<Long,Integer>(cid,1));
						}
					}
		});
		compStat = compStat.groupBy(0).reduce(new ReduceFunction<Tuple2<Long,Integer>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Integer> reduce(Tuple2<Long, Integer> value1,
					Tuple2<Long, Integer> value2) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Long,Integer>(value1.f0,value1.f1+value2.f1);
			}
		});
		DataSet<Tuple2<Long,Integer>> reducedComps = compStat.filter(new FilterFunction<Tuple2<Long,Integer>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(Tuple2<Long, Integer> value) throws Exception {
				// TODO Auto-generated method stub
				return value.f1>minSize&&value.f1<maxSize;
			}
			
		});
		
		/*
		 * consider only the maximum component for the given interval
		 */
		reducedComps = reducedComps.maxBy(1).reduce(new ReduceFunction<Tuple2<Long,Integer>>(){

			@Override
			public Tuple2<Long, Integer> reduce(Tuple2<Long, Integer> value1,
					Tuple2<Long, Integer> value2) throws Exception {
				
				return value1;
			}
			
		});
		
		
		
		DataSet<Tuple2<Long,Integer>> vertexCompCountCompSize = graph2.getVertices().flatMap(new FlatMapFunction<Vertex<Long,VertexValue>,
				Tuple2<Long,Long>>(){

					@Override
					public void flatMap(Vertex<Long, VertexValue> value,
							Collector<Tuple2<Long, Long>> out)
							throws Exception {
						for (long cid :value.f1.getCompIds()){
							out.collect(new Tuple2<Long,Long>(value.f0,cid));
						}
					}
			
		}).join(reducedComps).where(1).equalTo(0).with(new FlatJoinFunction<Tuple2<Long, Long>,Tuple2<Long,Integer>,
				Tuple2<Long,Integer>>(){

					@Override
					public void join(Tuple2<Long, Long> first,
							Tuple2<Long, Integer> second,
							Collector<Tuple2<Long, Integer>> out)
							throws Exception {
						out.collect(new Tuple2<Long,Integer>(first.f0,second.f1));
						
					}
		}) ;
		
		return  graph2.joinWithVertices(vertexCompCountCompSize, new VertexJoinFunction<VertexValue,Integer>(){

			@Override
			public VertexValue vertexJoin(VertexValue vertexValue,
					Integer inputValue) throws Exception {
					vertexValue.setConsidered(true);
				return vertexValue;
			}
			
		}).filterOnVertices(new FilterFunction<Vertex<Long,VertexValue>>(){

			@Override
			public boolean filter(Vertex<Long, VertexValue> value)
					throws Exception {
				return value.f1.isConsidered();
			}
			
		});
	}
}
