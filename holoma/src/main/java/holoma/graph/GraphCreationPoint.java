package holoma.graph;

import java.util.Set;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

import holoma.HolomaConstants;
import holoma.complexDatatypes.EdgeValue;
import holoma.complexDatatypes.VertexValue;
import holoma.parsing.ParsingPoint;

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
	ExecutionEnvironment env;
	
	/**
	 * Constructor.
	 */
	public GraphCreationPoint (ExecutionEnvironment env) {
		this.env=env;
	}
	
	
	/**
	 * Returns a graph which is created after parsing the specified ontology and mapping files.
	 * @param ontologyPath Path of the ontology files.
	 * @param ontologyFiles Names of the ontology files.
	 * @param mapFile Name of the mapping file (same path as ontology files).
	 * @param isOptimPrepr Specifies whether the preprocessor is optimistic: 'true' for optimistic, 'false' for pessimistic.
	 * An optimistic preprocessor adds missing edges' vertices to the set of all vertices.
	 * A pessimistic preprocessor deletes all edges where at least on vertex is not part of the set of all vertices.
	 * @param edgeFileLoc Where to print the edge file.
	 * @param vertexFileLoc Where to print the vertex file.
	 * @return The created graph.
	 */
	public Graph<String, VertexValue, EdgeValue> getGraphFromOntologyFiles (String path) {
		// #0: Load vertices and edges	 
		ParsingPoint pp = new ParsingPoint ();		
		if (HolomaConstants.IS_PRINTING_VALID_EDGVERT)
			pp.printEdgeVertexToFile(path);
		Set<Edge<String, EdgeValue>> edges = pp.getEdges(path);
		Set<Vertex<String, VertexValue>> vertices = pp.getVertices(path);
		if (HolomaConstants.IS_PRINTING_VALID_EDGVERT) {
			System.out.println();
			System.out.println("Printing "+edges.size()+" edges to file  ... ");
			System.out.println("Printing "+vertices.size()+" vertices to file ... ");			
		}
		
		 // #1: Creating the graph		 
		Graph<String, VertexValue, EdgeValue> graph = createGraph(edges, vertices);
		
		return graph;
	}
	
	
	/**
	 * Creates a graph based on a list of edges and vertices.
	 * @param edges Set of edges.
	 * @param vertices Set of vertices.
	 * @return The graph.
	 */
	private Graph<String, VertexValue, EdgeValue> createGraph (Set<Edge<String, EdgeValue>> edges, Set<Vertex<String, VertexValue>> vertices) {
		Graph<String, VertexValue, EdgeValue> graph = Graph.fromCollection(vertices, edges, env);
		return graph;
	}
	

	
	/** 
	 * Manages the creation of the graph.
	 * @return The created graph.
	 */
	public Graph<String, VertexValue, EdgeValue> getGraphFromEdgeVertexFile () {
		// load vertices and edges
		DataSet<Tuple2<String, VertexValue>> vertices = loadVertices(HolomaConstants.VERTEX_FILE_LOC);
		DataSet<Tuple3<String, String, EdgeValue>> edges = loadEdges(HolomaConstants.EDGE_FILE_LOC);
		// create graph with vertex ID type, vertex value type, and edge value type
		Graph<String, VertexValue, EdgeValue> graph = Graph.fromTupleDataSet(vertices, edges, env);
		return graph;
		
	}
	
	/**
	 * Loads the vertices of the graph.
	 * @param vertexFileLocation Location of the vertex file.
	 * @return The vertices of the graph. Format: (url, ont)
	 */
	public DataSet<Tuple2<String, VertexValue>> loadVertices(String vertexFileLocation) {
		DataSet<Tuple2<String, String>> vertexTuples = env.readCsvFile(vertexFileLocation)
				.fieldDelimiter("\t")  // configures the delimiter ("\t") that separates the fields within a row.
				.ignoreComments("#")  // configures the string ('#') that starts comments
				.types(String.class, String.class); // specifies the types for the CSV fields
		DataSet<Tuple2<String, VertexValue>> transformedVertex = vertexTuples.map(new MapFunction<Tuple2<String,String>,Tuple2<String,VertexValue>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = -2321185312262652017L;

			@Override
			public Tuple2<String, VertexValue> map(Tuple2<String, String> value)
					throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String,VertexValue> (value.f0,new VertexValue(value.f1,0));
			}
		});
				
	
		return transformedVertex;
	}
	
	
	/**
	 * Loads the edges of the graph.
	 * @param edgeFileLocation Location of the edge file.
	 * @return The edges of the graph. Format: (src, trg, type)
	 */
	public DataSet<Tuple3<String, String, EdgeValue>> loadEdges(final String edgeFileLocation) {		
		DataSet<Tuple3<String, String, Integer>> edgeTuples = env.readCsvFile(edgeFileLocation)
				.fieldDelimiter("\t")  // configures the delimiter ("\t") that separates the fields within a row.
	            .ignoreComments("#")  // configures the string ('#') that starts comments
	            .types(String.class, String.class, Integer.class); // specifies the types for the CSV fields
		DataSet<Tuple3<String, String, EdgeValue>> customEdges = edgeTuples.map(new MapFunction<Tuple3<String, String, Integer>,
				Tuple3<String,String,EdgeValue>> (){

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple3<String, String, EdgeValue> map(
							Tuple3<String, String, Integer> arg0)
							throws Exception {
						Tuple3 <String, String,EdgeValue> tt = new Tuple3<String,String,EdgeValue>();
						tt.f0 = arg0.f0;
						tt.f1 = arg0.f1;
						if (arg0.f2 == 1 ||arg0.f2 ==2){
							EdgeValue ev= new EdgeValue(arg0.f2, 0.5f, edgeFileLocation);
							tt.f2 = ev;
						}else {
							EdgeValue ev= new EdgeValue(arg0.f2, 1f, edgeFileLocation);
							tt.f2 = ev;
						}
						
						return tt;
					}

					

			
			
		});
		
		return customEdges;
	}
	
	
	
	
	           
}
