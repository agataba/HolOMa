package holoma;

import java.util.Set;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

/**
 * This class creates the graph which
 * consists of ontologies and correspondences.
 * A <b>vertex</b> is defined as a tuple (url, ont)
 * with <i>url</i> its URL/ ID
 * and <i>ont</i> the name of ontology to which it belongs to.
 * An <b>edged</b> is defined as a triple (src, trg, type)
 * with <i>src</i> the source, <i>trg</i> the target of the edge
 * and <i>type</i> the type of the edge.
 * An edge's <b>type</b> is '0' for <i>equal</i>, '1' for <i>is-a</i>.
 * (The weight of an edge is a function from the set of types to the real numbers.)
 * @author max
 *
 */
public class GraphCreationPoint {
	
	
	
	/** Context in which the program is currently executed. */
	public ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	
	
	
	/**
	 * Constructor.
	 */
	public GraphCreationPoint () {
		
	}
	
	/**
	 * Creates a graph based on a list of edges and vertices.
	 * @param edges Set of edges.
	 * @param vertices Set of vertices.
	 * @return The graph.
	 */
	public Graph<String, String, Integer> createGraph (Set<Edge<String, Integer>> edges, Set<Vertex<String, String>> vertices) {
		Graph<String, String, Integer> graph = Graph.fromCollection(vertices, edges, env);
		return graph;
	}
	

	
	/** 
	 * Manages the creation of the graph.
	 * @param edgeFileLocation Location of the edge file.
	 * @param vertexFileLocation Location of the vertex file.
	 * @return The graph with vertex ID type, vertex value type, and edge value type as String.
	 */
	public Graph<String, String, Integer> createGraphFromFile (String edgeFileLocation, String vertexFileLocation) {
		// load vertices and edges
		DataSet<Tuple2<String, String>> vertices = loadVertices(vertexFileLocation);
		DataSet<Tuple3<String, String, Integer>> edges = loadEdges(edgeFileLocation);
		// create graph with vertex ID type, vertex value type, and edge value type
		Graph<String, String, Integer> graph = Graph.fromTupleDataSet(vertices, edges, env);
		return graph;
		
	}
	
	/**
	 * Loads the vertices of the graph.
	 * @param vertexFileLocation Location of the vertex file.
	 * @return The vertices of the graph. Format: (url, ont)
	 */
	public DataSet<Tuple2<String, String>> loadVertices(String vertexFileLocation) {
		DataSet<Tuple2<String, String>> vertexTuples = env.readCsvFile(vertexFileLocation)
				.fieldDelimiter("\t")  // configures the delimiter ("\t") that separates the fields within a row.
				.ignoreComments("#")  // configures the string ('#') that starts comments
				.types(String.class, String.class); // specifies the types for the CSV fields
	
		return vertexTuples;
	}
	
	
	/**
	 * Loads the edges of the graph.
	 * @param edgeFileLocation Location of the edge file.
	 * @return The edges of the graph. Format: (src, trg, type)
	 */
	public DataSet<Tuple3<String, String, Integer>> loadEdges(String edgeFileLocation) {		
		DataSet<Tuple3<String, String, Integer>> edgeTuples = env.readCsvFile(edgeFileLocation)
				.fieldDelimiter("\t")  // configures the delimiter ("\t") that separates the fields within a row.
	            .ignoreComments("#")  // configures the string ('#') that starts comments
	            .types(String.class, String.class, Integer.class); // specifies the types for the CSV fields
		
		return edgeTuples;
	}
	
	
	
	
	           
}
