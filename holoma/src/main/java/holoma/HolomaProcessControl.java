package holoma;
import java.util.List;
import java.util.Map;

//
import org.apache.commons.lang.time.StopWatch;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

/**
 * This class manages the overall workflow 
 * of holistic ontology mapping.
 * @author max
 *
 */
public class HolomaProcessControl {
	
	// TODO: file location the property file
	private static final String edgeFileLoc = "/home/max/git/HolOMa/holoma/src/main/resources/example_edges.csv";
	private static final String vertexFileLoc = "/home/max/git/HolOMa/holoma/src/main/resources/example_vertices.csv";
	private static final String conCompFileLoc = "/home/max/git/HolOMa/holoma/src/main/resources/example_connectedComponents.csv";

	private static final boolean noSingletonComponents = true;
	
	
	public static void main(String[] args) {
		StopWatch stopWatch = new StopWatch();
		
		System.out.println("HolOMa (Holistic Ontology Mapping)\n"
				+ "-------------------------------------------------------------");		
		System.out.println("Properties:");
		System.out.println("edge file location: "+edgeFileLoc);
		System.out.println("vertex file location: "+vertexFileLoc);
		System.out.println("no singleton components: "+noSingletonComponents);
		
		System.out.println("Creating the graph ... ");
		stopWatch.start();
		GraphCreationPoint creationPoint = new GraphCreationPoint(edgeFileLoc, vertexFileLoc);
		Graph<String, String, Integer> graph = creationPoint.createGraph();
		stopWatch.stop(); System.out.println("\t in "+stopWatch.getTime()+"ms");
		System.out.println("Showing the graph ... ");
		GraphVisualisation.showEdgesVertices(graph);	
		
		System.out.println("\nEvaluating the graph ... ");
		descriptEvaluate(graph);
		
		System.out.println("\nConnected Components:");
		Map<Long, List<String>> conCompts = calculateConnComponents(graph);
//		GraphVisualisation.showConnectedComponents(conCompts);
		System.out.println("printing to "+conCompFileLoc+" ... ");
		GraphVisualisation.printConnectedComponents(conCompts, conCompFileLoc);
		
		
		System.out.println("--- End ---");
	}
	
	
	
	/**
	 * Calculates the connected components and changes their format to a map
	 * which has the component ID as key, and the nodes of this ID as value.
	 * @param graph The components are based on the graph.
	 * @return Connected Components.
	 */
	private static Map<Long, List<String>> calculateConnComponents (Graph<String, String, Integer> graph){
		GraphEvaluationPoint eval = new GraphEvaluationPoint(graph);
		DataSet<Vertex<String, Long>> verticesWithComponents = eval.getConnectedComponents();
		return GraphVisualisation.sortConnectedComponents(verticesWithComponents, noSingletonComponents);
	}
	
	
	/**
	 * Descriptively evaluates the graph.
	 * @param graph The graph.
	 */
	private static void descriptEvaluate (Graph<String, String, Integer> graph) {
		GraphEvaluationPoint eval = new GraphEvaluationPoint(graph);
		StopWatch stopWatch = new StopWatch();
		stopWatch.reset();stopWatch.start();
		System.out.println("Validation: "+eval.validateGraph());
		stopWatch.stop(); System.out.println("\t in "+stopWatch.getTime()+"ms");
		stopWatch.reset();stopWatch.start();
		System.out.println("#vertices: "+eval.getCountVertices());
		stopWatch.stop(); System.out.println("\t in "+stopWatch.getTime()+"ms");
		stopWatch.reset();stopWatch.start();
		System.out.println("#edges: "+eval.getCountEdges());
		stopWatch.stop(); System.out.println("\t in "+stopWatch.getTime()+"ms");	
	}
	
	
	

}