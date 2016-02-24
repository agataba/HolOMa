package holoma.ppr;

import holoma.complexDatatypes.EdgeValue;
import holoma.complexDatatypes.VertexValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

public class OverallSumCalculator {

	Logger log = Logger.getLogger(getClass());
	
	public DataSet<Tuple2<String, Map<Long, Float>>> getOverallSumDataSet (boolean reverse, Graph<String,VertexValue,EdgeValue>graph){
		DataSet<Tuple2<String,Long>> vertex2Componet = graph.getVertices().flatMap(new FlatMapFunction<Vertex<String,VertexValue>,Tuple2<String,Long>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = -858579924046225915L;

			@Override
			public void flatMap(Vertex<String, VertexValue> value,
					Collector<Tuple2<String, Long>> out) throws Exception {
				for (Long c: value.getValue().getCompIds()){
					Tuple2 <String,Long> t = new Tuple2<String,Long>(value.f0,c);
							out.collect(t);
				}
				
			}
		});
		
		DataSet<Tuple2<Edge<String,EdgeValue>,Long>> srcJoin=graph.getEdges().join(vertex2Componet).where(0).equalTo(0).map(
				new MapFunction<Tuple2<Edge<String,EdgeValue>,Tuple2<String,Long>>,Tuple2<Edge<String,EdgeValue>,Long>>(){

					/**
					 * 
					 */
					private static final long serialVersionUID = -5761127274730107791L;

					@Override
					public Tuple2<Edge<String, EdgeValue>, Long> map(
							Tuple2<Edge<String, EdgeValue>, Tuple2<String, Long>> value)
							throws Exception {
						return new Tuple2<Edge<String,EdgeValue>,Long>(value.f0,value.f1.f1);
					}
				});
		DataSet<Tuple3<String,Long,Float>> edgeWeightPerComp = srcJoin.join(vertex2Componet).where("f0.f1","f1").equalTo("f0","f1").map(new MapFunction<Tuple2<Tuple2<Edge<String,EdgeValue>,Long>,
				Tuple2<String,Long>>,Tuple3<String,Long,Float>>(){

					/**
					 * 
					 */
					private static final long serialVersionUID = -5424974801824775101L;

					@Override
					public Tuple3<String, Long, Float> map(
							Tuple2<Tuple2<Edge<String, EdgeValue>, Long>, Tuple2<String, Long>> value)
							throws Exception {
						return new Tuple3 <String,Long,Float>(value.f0.f0.f0,value.f0.f1,value.f0.f0.f2.getWeight());
					}
			
		});
		DataSet<Tuple2<String,Map<Long,Float>>> sumPerComponent = edgeWeightPerComp.groupBy(0,1).reduce(new ReduceFunction<Tuple3<String,Long,
				Float>>(){

					/**
					 * 
					 */
					private static final long serialVersionUID = 2968214157273490216L;

					@Override
					public Tuple3<String, Long, Float> reduce(
							Tuple3<String, Long, Float> value1,
							Tuple3<String, Long, Float> value2)
							throws Exception {
						Tuple3 <String,Long,Float> agg = new Tuple3<String,Long,Float>(value1.f0,value1.f1,value1.f2+value2.f2);
						return agg;
					}
		}).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple3<String,Long,Float>,Tuple2<String,Map<Long,Float>>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 8258921365276714687L;

			@Override
			public void reduce(Iterable<Tuple3<String, Long, Float>> values,
					Collector<Tuple2<String, Map<Long, Float>>> out)
					throws Exception {
				Map<Long,Float> overallMap = new HashMap<Long,Float>();
				String v = null ;
				for (Tuple3<String, Long, Float> t : values){
					overallMap.put(t.f1, t.f2);
					v = t.f0;
				}
				out.collect(new Tuple2<String,Map<Long,Float>>(v,overallMap));
			}
		});
		return sumPerComponent;
	}
}
