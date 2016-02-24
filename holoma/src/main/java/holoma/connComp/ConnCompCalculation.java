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
	/** The graph. */
	private  transient  Graph<String, VertexValue, EdgeValue> GRAPH;	
	/** Graph for calculating connected components. */
	private  transient Graph<String, Long, EdgeValue> componentGraph; 
	/** Map from component ID to its set of vertex IDs.*/
	private Map<Long, Set<String>> connCompts = new HashMap<Long, Set<String>>();
	
	private ExecutionEnvironment env;
	private 	DataSet<Vertex<String, Long>> comComptsDataset;
	
	
	
	
	
	
	public ConnCompCalculation(Graph<String, VertexValue, EdgeValue> graph2, ExecutionEnvironment env) {
		this.GRAPH = graph2;
		this.env =env;
	}



	/**
	 * Returns the graph on which the connected components are calculated.
	 * @return Graph for connected components calculation.
	 */
	public Graph<String, Long, EdgeValue> getComponentGraph () { return this.componentGraph; }
	
		
	
	/**
	 * Naive approach: calculate connected components on the whole graph of all ontologies.
	 * @return Map from component ID to a connected component, i.e., a set of vertices.
	 */
	public Map<Long, Set<String>> calculateConnComp_naive () {
		try {
			// exactly one ontology: no connected components
			connCompts = (HolomaConstants.ONTOLOGY_FILES.length<=1) ? null : getSortedConnComp ();
			
		} catch (Exception e){
			System.err.println("Error while calculating connected components.");
			e.printStackTrace();
			System.out.println("\nQuit program ... ");
			System.exit(1);
		}
		
		return connCompts;				
	}
	
	public DataSet<Vertex<String, Long>> calculateConnComp () {
		try {
			// exactly one ontology: no connected components
			this.comComptsDataset = this.getConnectedComponents();
			
		} catch (Exception e){
			System.err.println("Error while calculating connected components.");
			e.printStackTrace();
			System.out.println("\nQuit program ... ");
			System.exit(1);
		}
		
		return comComptsDataset;				
	}
	
	
	/**
	 * Calculates the connected components and changes their format to a map
	 * which has the component ID as key, and the nodes of this ID as value.
	 * @param noSingletonComponents Singletons of connected components are eliminated iff 'true'.
	 * @return Connected Components.
	 */
	private Map<Long, Set<String>> getSortedConnComp (){
		DataSet<Vertex<String, Long>> verticesWithComponents = getConnectedComponents();			
		return sortConnectedComponents(verticesWithComponents);
	}
	
	
	
	/**
	 * Returns the connected components of the given graph.
	 * @return DataSet of vertices, where the vertex values correspond to the component ID.
	 */
	private DataSet<Vertex<String, Long>> getConnectedComponents () {
		// set the degree option to true
		VertexCentricConfiguration parameters = new VertexCentricConfiguration();
		parameters.setOptDegrees(true);
		
		DataSet<Vertex<String, Long>> verticesWithComponents = null;		
		
		this.GRAPH = this.GRAPH.filterOnEdges(
				new FilterFunction<Edge<String, EdgeValue>>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1505232572947290638L;

					public boolean filter(Edge<String, EdgeValue> edge) {
						// keep only edges that denotes an equal relation
						return (edge.getValue().type == 0);
					}
				});
		
		// #1: Initialize each vertex value with its own and unique component ID
		
		System.setProperty("sun.io.serialization.extendedDebugInfo","true");
		DataSet<Edge<String,EdgeValue>> filteredEdges = GRAPH.getEdges().filter(new FilterFunction<Edge<String,EdgeValue>>(){

			@Override
			public boolean filter(Edge<String, EdgeValue> value)
					throws Exception {
				// TODO Auto-generated method stub
				return value.getValue().getType()==0;
			}
			
		});
		DataSet<Vertex<String,Long>> corNodes = GRAPH.getVertices().join(GRAPH.getEdgeIds()).where(0).
				equalTo(0).with(new FlatJoinFunction<Vertex<String,VertexValue>,Tuple2<String,String>,Vertex<String,Long>>(){

					@Override
					public void join(Vertex<String, VertexValue> first,
							Tuple2<String, String> second,
							Collector<Vertex<String, Long>> out)
							throws Exception {
						Vertex<String,Long> v = new Vertex <String,Long> ();
						v.f0 = first.f0;
						v.f1 = (long) first.f1.getConComponent();
						out.collect(v);
					}
					
				}).distinct().union(GRAPH.getVertices().join(GRAPH.getEdgeIds()).where(0).
						equalTo(1).with(new FlatJoinFunction<Vertex<String,VertexValue>,Tuple2<String,String>,Vertex<String,Long>>(){

							@Override
							public void join(Vertex<String, VertexValue> first,
									Tuple2<String, String> second,
									Collector<Vertex<String, Long>> out)
									throws Exception {
								Vertex<String,Long> v = new Vertex <String,Long> ();
								v.f0 = first.f0;
								v.f1 = (long) first.f1.getConComponent();
								out.collect(v);
								
							}
							
						}).distinct());
		
		
		componentGraph = Graph.fromDataSet(corNodes, filteredEdges, env);
		
		
		//this.GRAPH.joinWithVertices(inputDataSet, vertexJoinFunction)		
		// #2: create subgraph: only edges with value "equal"
		

		// #3: calculate the connected components
		try {
			verticesWithComponents = this.componentGraph.run(
					new ConnectedComponents<String, EdgeValue>(HolomaConstants.MAX_ITER)
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
