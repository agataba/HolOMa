package holoma;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;

/**
 * This class provides methods for 
 * determining connected components.
 * @author max
 *
 */
@SuppressWarnings("serial")
public class GraphEvaluationPoint implements Serializable {
	/** The graph. */
	transient private final Graph<String, String, Integer> GRAPH;	
	/** Graph for calculating connected components. */
	transient private Graph<String, Long, Integer> componentGraph; 
	
	
	/**
	 * Constructor.
	 * @param graph The graph which shall be evaluated.
	 */
	public GraphEvaluationPoint (Graph<String, String, Integer> graph) {
		this.GRAPH = graph;
	}
	
	
	
	/**
	 * Returns the graph on which the connected components are calculated.
	 * @return Graph for connected components calculation.
	 */
	public Graph<String, Long, Integer> getComponentGraph () { return this.componentGraph; }
	
	
	/**
	 * Returns the connected components of the given graph.
	 * @return DataSet of vertices, where the vertex values correspond to the component ID.
	 */
	private DataSet<Vertex<String, Long>> getConnectedComponents () {
		// set the degree option to true
		VertexCentricConfiguration parameters = new VertexCentricConfiguration();
		parameters.setOptDegrees(true);
		
		DataSet<Vertex<String, Long>> verticesWithComponents = null;		
		
		// #1: Initialize each vertex value with its own and unique component ID
		this.componentGraph = this.GRAPH.mapVertices(
				new MapFunction<Vertex<String, String>, Long>() {
					public Long map (Vertex<String, String> value) {
						// the component ID is the hashCode of the key
						return (long) value.getId().hashCode();
					}
				});	
				
		// #2: create subgraph: only edges with value "equal"
		this.componentGraph = this.componentGraph.filterOnEdges(
				new FilterFunction<Edge<String, Integer>>() {
					public boolean filter(Edge<String, Integer> edge) {
						// keep only edges that denotes an equal relation
						return (edge.getValue() == 0);
					}
				});

		// #3: calculate the connected components
		try {
			verticesWithComponents = this.componentGraph.run(
					new ConnectedComponents<String, Integer>(HolomaConstants.MAX_ITER)
					);
		} catch (Exception e) {
			e.printStackTrace();
		}		
		
		return verticesWithComponents;
	}
	
	
	/**
	 * Calculates the connected components and changes their format to a map
	 * which has the component ID as key, and the nodes of this ID as value.
	 * @param noSingletonComponents Singletons of connected components are eliminated iff 'true'.
	 * @return Connected Components.
	 */
	public Map<Long, Set<String>> calculateConnComponents (){
		DataSet<Vertex<String, Long>> verticesWithComponents = getConnectedComponents();			
		return GraphVisualisation.sortConnectedComponents(verticesWithComponents);
	}
	
	
	
	
	
	
	
	/**
	 * Analyzes the connected components.
	 * @param connComp Map of connected components.
	 */
	public static void analyseConnComponents (Map<Long, Set<String>> connComp) {
		int count = connComp.size();
		int max = 0, min = Integer.MAX_VALUE, sum = 0;
		Map<Integer, Long> histogramData = new HashMap<Integer, Long>();
		
		for (long component : connComp.keySet()) {
			int size = connComp.get(component).size();
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
		System.out.println("-------------------------------------------------------------");
		System.out.println("Analysis of connected components (#nodes):");
		System.out.println("count:     "+count);
		System.out.println("avg:       "+avg);
		System.out.println("min:       "+((min==Integer.MAX_VALUE) ? "--" : min));
		System.out.println("max:       "+((max==0) ? "--" : max));
		System.out.println();
		System.out.println("size \t|\tcount\n-----------------------");
		for (int size : histogramData.keySet())
			System.out.println(" "+size+"\t|\t "+histogramData.get(size));
		System.out.println();
	}
	
}
