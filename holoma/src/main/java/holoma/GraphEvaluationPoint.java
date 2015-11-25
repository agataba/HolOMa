package holoma;

import java.io.Serializable;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.graph.validation.InvalidVertexIdsValidator;

/**
 * This class evaluates a given graph regarding to
 * some selected measures and characteristics.
 * @author max
 *
 */
@SuppressWarnings("serial")
public class GraphEvaluationPoint implements Serializable {
	/** The graph. */
	transient private final Graph<String, String, Integer> GRAPH;
	/** Maximum number of iteration steps for connected components. */
	private final int maxIterations;
	
	/**
	 * Constructor.
	 * @param graph The graph which shall be evaluated.
	 */
	public GraphEvaluationPoint (Graph<String, String, Integer> graph, int maxIterations) {
		this.GRAPH = graph;
		this.maxIterations = maxIterations;
	}
	
	/**
	 * Checks that the edge set input contains valid vertex IDs, i.e. that they also exist in the vertex input set.
	 * @return 'true' iff edges contain valid vertex IDs; otherwise 'false'.
	 */
	public boolean validateGraph() {
		boolean v = false;
		try {
			v = this.GRAPH.validate(new InvalidVertexIdsValidator<String, String, Integer>());
		} catch (Exception e) {
			System.err.println("Exception while valiadting the graph.");
			e.printStackTrace();
		} 
		return v;
	}
	
	
	/**
	 * Get the number of vertices.
	 * @return Number of vertices.
	 */
	public long getCountVertices () {
		long c = -1;
		try {
			c = this.GRAPH.numberOfVertices();
		} catch (Exception e) {
			System.err.println("Exception while counting vertices!");
			e.printStackTrace();
			System.exit(1);
		}
		return c;
	}
	
	/**
	 * Get the number of edges.
	 * @return Number of Edges.
	 */
	public long getCountEdges () {
		long c = -1;
		try {
			c = this.GRAPH.numberOfEdges();
		} catch (Exception e) {
			System.err.println("Exception while counting edges!");
			e.printStackTrace();
			System.exit(1);
		}
		return c;
	}
	
	
	/**
	 * Returns the connected components of the given graph.
	 * @return DataSet of vertices, where the vertex values correspond to the component ID.
	 */
	public DataSet<Vertex<String, Long>> getConnectedComponents () {
		DataSet<Vertex<String, Long>> verticesWithComponents = null;
		
		
		// #1: initialise each vertex value with its own and unique component ID
		Graph<String, Long, Integer> componentGraph = this.GRAPH.mapVertices(
				new MapFunction<Vertex<String, String>, Long>() {
					public Long map (Vertex<String, String> value) {
						// the component ID is the hashCode of the key
						return (long) value.getId().hashCode();
					}
				});
	
		// #2: create subgraph: only edges with value "equal"
		componentGraph = componentGraph.filterOnEdges(
				new FilterFunction<Edge<String, Integer>>() {
					public boolean filter(Edge<String, Integer> edge) {
						// keep only edges that denotes an equal relation
						return (edge.getValue() == 0);
					}
				});

		// #3: calculate the connected components
		try {
			verticesWithComponents = componentGraph.run(
					new ConnectedComponents<String, Integer>(this.maxIterations)
					);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		return verticesWithComponents;
	}
	
}
