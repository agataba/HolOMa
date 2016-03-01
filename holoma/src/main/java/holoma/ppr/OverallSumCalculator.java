package holoma.ppr;

import holoma.complexDatatypes.EdgeValue;
import holoma.complexDatatypes.VertexValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

public class OverallSumCalculator {

	Logger log = Logger.getLogger(getClass());
	
	public DataSet<Tuple2<Long, Map<Long, Float>>> getOverallSumDataSet (boolean reverse, Graph<Long, VertexValue, Float> reducedGraph){
		DataSet<Tuple2<Long,Long>> vertex2Componet = reducedGraph.getVertices().flatMap(new FlatMapFunction<Vertex<Long,VertexValue>,Tuple2<Long,Long>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = -858579924046225915L;

			@Override
			public void flatMap(Vertex<Long, VertexValue> value,
					Collector<Tuple2<Long, Long>> out) throws Exception {
				for (Long c: value.getValue().getCompIds()){
					//vertex id, component id
					Tuple2 <Long,Long> t = new Tuple2<Long,Long>(value.f0,c);
							out.collect(t);
				}
				
			}
		});
		
		DataSet<Tuple4<Long,Long,Long,Float>> srcJoin=reducedGraph.getEdges().join(vertex2Componet).where(0).equalTo(0).with(
				new FlatJoinFunction<Edge<Long,Float>,Tuple2<Long,Long>,Tuple4<Long,Long,Long,Float>>(){

					@Override
					public void join(Edge<Long, Float> first,
							Tuple2<Long, Long> second,
							Collector<Tuple4<Long, Long, Long, Float>> out)
							throws Exception {
						out.collect(new Tuple4<Long, Long, Long, Float>(second.f0,first.f1,second.f1,first.f2));
						
					}
					
				});
		DataSet<Tuple3<Long,Long,Float>> groupedSrc = srcJoin.join(vertex2Componet).where(1,2).equalTo(0,1).with(
				new FlatJoinFunction<Tuple4<Long,Long,Long,Float>,Tuple2<Long,Long>,Tuple3<Long,Long,Float>>(){

					@Override
					public void join(Tuple4<Long, Long, Long, Float> first,
							Tuple2<Long, Long> second,
							Collector<Tuple3<Long, Long, Float>> out)
							throws Exception {
						out.collect(new Tuple3<Long,Long,Float> (first.f0,second.f1,first.f3));
						
					}
					
				});
				
		//group by src and component
		DataSet<Tuple2<Long,Map<Long,Float>>> sumPerComponent = groupedSrc.groupBy(0,1).reduce(
				new ReduceFunction<Tuple3<Long,Long,Float>>(){

					/**
					 * 
					 */
					private static final long serialVersionUID = 2968214157273490216L;

					@Override
					public Tuple3<Long, Long, Float> reduce(
							Tuple3<Long, Long, Float> value1,
							Tuple3<Long, Long, Float> value2)
							throws Exception {
						Tuple3 <Long,Long,Float> agg = new Tuple3<Long,Long,Float>(value1.f0,value1.f1,value1.f2+value2.f2);
						return agg;
					}
		}).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple3<Long,Long,Float>,Tuple2<Long,Map<Long,Float>>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 8258921365276714687L;

			@Override
			public void reduce(Iterable<Tuple3<Long, Long, Float>> values,
					Collector<Tuple2<Long, Map<Long, Float>>> out)
					throws Exception {
				Map<Long,Float> overallMap = new HashMap<Long,Float>();
				Long v = null ;
				for (Tuple3<Long, Long, Float> t : values){
					overallMap.put(t.f1, t.f2);
					v = t.f0;
				}
				out.collect(new Tuple2<Long,Map<Long,Float>>(v,overallMap));
			}
		});
		return sumPerComponent;
	}
}
