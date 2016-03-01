package holoma.connComp;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import holoma.HolomaConstants;
import holoma.complexDatatypes.EdgeValue;
import holoma.complexDatatypes.VertexValue;

/**
 * This class provides methods for 
 * determining connected components.
 * @author max
 *
 */

public class ConnCompCalculation  implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7414570588346708760L;
	transient Logger log = Logger.getLogger(getClass());
	
	private Map<Long, Set<String>> connCompts = new HashMap<Long, Set<String>>();
	
	private ExecutionEnvironment env;
	
	
	
	
	/**
	 * Returns the connected components of the given graph.
	 * @return DataSet of vertices, where the vertex values correspond to the component ID.
	 */
	public DataSet<Vertex<Long, Long>> getConnectedComponents (Graph<Long,VertexValue,Float> graph) {
		// set the degree option to true
		VertexCentricConfiguration parameters = new VertexCentricConfiguration();
		parameters.setOptDegrees(true);
		
		DataSet<Vertex<Long, Long>> verticesWithComponents = null;		
		
		
		System.setProperty("sun.io.serialization.extendedDebugInfo","true");
		DataSet<Edge<Long,Float>> filteredEdges = graph.getEdges().filter(new FilterFunction<Edge<Long,Float>>(){

			private static final long serialVersionUID = 1505232572947290638L;

			@Override
			public boolean filter(Edge<Long, Float> value)
					throws Exception {
				// TODO Auto-generated method stub
				return value.f2==1;
			}
			
		});
		DataSet<Vertex<Long,Long>> corNodes = graph.getVertices().join( filteredEdges).where(0).
				equalTo(0).with(new FlatJoinFunction<Vertex<Long,VertexValue>,Edge<Long,Float>,Vertex<Long,Long>>(){

					@Override
					public void join(Vertex<Long, VertexValue> first,
							Edge<Long,Float> second,
							Collector<Vertex<Long, Long>> out)
							throws Exception {
						Vertex<Long,Long> v = new Vertex <Long,Long> ();
						v.f0 = first.f0;
						v.f1 = (long) first.f1.getConComponent();
						out.collect(v);
					}
					
				}).distinct().union(graph.getVertices().join(filteredEdges).where(0).
						equalTo(1).with(new FlatJoinFunction<Vertex<Long,VertexValue>,Edge<Long,Float>,Vertex<Long,Long>>(){

							@Override
							public void join(Vertex<Long, VertexValue> first,
									Edge<Long,Float> second,
									Collector<Vertex<Long, Long>> out)
									throws Exception {
								Vertex<Long,Long> v = new Vertex <Long,Long> ();
								v.f0 = first.f0;
								v.f1 = (long) first.f1.getConComponent();
								out.collect(v);
								
							}
							
						}).distinct());
		
		
		Graph<Long,Long,Float> componentGraph = Graph.fromDataSet(corNodes, filteredEdges, env);
		
		
		//this.GRAPH.joinWithVertices(inputDataSet, vertexJoinFunction)		
		// #2: create subgraph: only edges with value "equal"
		

		// #3: calculate the connected components
		try {
			verticesWithComponents = componentGraph.run(
					new ConnectedComponents<Long, Float>(HolomaConstants.MAX_ITER)
					);
		} catch (Exception e) {
			e.printStackTrace();
		}		
		return verticesWithComponents;
	}
	
	
	
	/**
	 * Sorts the nodes according to their association with a connected component. 
	 * All nodes with the same component ID are grouped together. They are mapped
	 * to their component ID.
	 * @param verticesWithComponents Vertices with the connected component ID as value.
	 * @param noSingletons Singletons of connected components are eliminated iff 'true'.
	 * @return Map from component ID to its set of connected vertices.
	 */
	public Map<Long, Set<String>> sortConnectedComponents (DataSet<Vertex<String, Long>> verticesWithComponents) {
		
		try {
			for (Vertex<String, Long> vertex : verticesWithComponents.collect()) {
				
				// component has already occurred: add entry to existing hash map
				if (connCompts.keySet().contains(vertex.f1))
					connCompts.get(vertex.f1).add(vertex.f0);
				// component is new: create a new entry in the hash map
				else {
					Set<String> l = new HashSet<String>();
					l.add(vertex.f0);
					connCompts.put(vertex.f1, l);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if (HolomaConstants.NO_SINGLETON_CONNCOMP)
			connCompts = eliminateSingletons ();
		
		return connCompts;
	}
	
	
	/**
	 * Eliminates singletons from the set of connected components.
	 * @param connCompts Set of connected components (potentially components of cardinality one).
	 * @return Set of connected components such that each connected component contains at least two nodes.
	 */
	private Map<Long, Set<String>> eliminateSingletons () {
		Map<Long, Set<String>> newMap = new HashMap<Long, Set<String>>();
		for (Long key : this.connCompts.keySet()) {
			if (this.connCompts.get(key).size() > 1)
				newMap.put(key, this.connCompts.get(key));
		}		
		return newMap;		
	}
	
	
	/**
	 * Analyzes the connected components.
	 * @return Analysis result.
	 */
	public String analyseConnComponents () {
		int count = this.connCompts.size();
		int max = 0, min = Integer.MAX_VALUE, sum = 0;
		Map<Integer, Long> histogramData = new HashMap<Integer, Long>();
		
		for (long component : this.connCompts.keySet()) {
			int size = this.connCompts.get(component).size();
			if (histogramData.containsKey(size)) {
				long value = histogramData.get(size);
				histogramData.put(size, (value+1));
			}
			else
				histogramData.put(size, 1l);
			sum += size;
			max = (size > max) ? size : max;
			min = (size < min) ? size : min;
		}
		float avg = sum / (1.0f*count);

		String result = "-------------------------------------------------------------\n";
		result += "Analysis of connected components (#nodes):\n";
		result += "count:     "+count+"\n";
		result += "avg:       "+avg+"\n";
		result += "min:       "+((min==Integer.MAX_VALUE) ? "--" : min)+"\n";
		result += "max:       "+((max==0) ? "--" : max)+"\n";
		result += "\n";
		result += "size \t|\tcount\n-----------------------\n";
		for (int size : histogramData.keySet())
			result += " "+size+"\t|\t "+histogramData.get(size)+"\n";
		result += "\n";
		
		return result;
	}
	
}
