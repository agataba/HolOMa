package holoma;

import org.apache.flink.graph.Graph;

/**
 * This class evaluates a given graph regarding to
 * some selected measures and characteristics.
 * @author max
 *
 */
public class GraphEvaluationPoint {
	/** The graph. */
	private final Graph<String, String, String> GRAPH;
	
	/**
	 * Constructor.
	 * @param graph The graph which shall be evaluated.
	 */
	public GraphEvaluationPoint (Graph<String, String, String> graph) {
		this.GRAPH = graph;
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
			e.printStackTrace();
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
			e.printStackTrace();
		}
		return c;
	}
	
}
