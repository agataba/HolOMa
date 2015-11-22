package holoma;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.log4j.Logger;

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
	
	/** Log4j message logger. */
	static Logger log = Logger.getLogger("GraphCreationPoint");
	
	/** Context in which the program is currently executed. */
	private ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	/** Location of the edges' file. */
	private final String EDGE_FILE;
	/** Location of the vertices file. */
	private final String VERTEX_FILE;
	
	
	
	
	/**
	 * Constructor.
	 */
	public GraphCreationPoint (String edgeFileLocation, String vertexFileLocation) {
		this.EDGE_FILE = edgeFileLocation;
		this.VERTEX_FILE = vertexFileLocation;
	}
	
	
	
	
	/** 
	 * Manages the creation of the graph.
	 * @return The graph with vertex ID type, vertex value type, and edge value type as String.
	 */
	public Graph<String, String, Integer> createGraph () {
		// load vertices and edges
		DataSet<Tuple2<String, String>> vertices = loadVertices();
		DataSet<Tuple3<String, String, Integer>> edges = loadEdges();
		// create graph with vertex ID type, vertex value type, and edge value type
		Graph<String, String, Integer> graph = Graph.fromTupleDataSet(vertices, edges, env);
		return graph;
		
	}
	
	/**
	 * Loads the vertices of the graph.
	 * @return The vertices of the graph. Format: (url, ont)
	 */
	private DataSet<Tuple2<String, String>> loadVertices() {
		DataSet<Tuple2<String, String>> vertexTuples = env.readCsvFile(this.VERTEX_FILE)
				.fieldDelimiter("\t")  // configures the delimiter ("\t") that separates the fields within a row.
				.ignoreComments("#")  // configures the string ('#') that starts comments
				.types(String.class, String.class); // specifies the types for the CSV fields
	
		return vertexTuples;
	}
	
	
	/**
	 * Loads the edges of the graph.
	 * @return The edges of the graph. Format: (src, trg, type)
	 */
	private DataSet<Tuple3<String, String, Integer>> loadEdges() {		
		DataSet<Tuple3<String, String, Integer>> edgeTuples = env.readCsvFile(this.EDGE_FILE)
				.fieldDelimiter("\t")  // configures the delimiter ("\t") that separates the fields within a row.
	            .ignoreComments("#")  // configures the string ('#') that starts comments
	            .types(String.class, String.class, Integer.class); // specifies the types for the CSV fields
		
		return edgeTuples;
	}
	
	           
}
