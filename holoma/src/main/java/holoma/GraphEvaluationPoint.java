package holoma;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.validation.InvalidVertexIdsValidator;

/**
 * This class evaluates a given graph regarding to
 * some selected measures and characteristics.
 * @author max
 *
 */
public class GraphEvaluationPoint {
	/** The graph. */
	private final Graph<String, String, Integer> GRAPH;
	
	/**
	 * Constructor.
	 * @param graph The graph which shall be evaluated.
	 */
	public GraphEvaluationPoint (Graph<String, String, Integer> graph) {
		this.GRAPH = graph;
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
		// #1: initialise the each vertex value with its own component ID
		
		// #2: create subgraph: undirected, only edges with value "equal"
		
		// #3: calculate the connected components
		
		return verticesWithComponents;
	}
	
	
	
	
}
