package holoma;
//
import org.apache.commons.lang.time.StopWatch;
import org.apache.flink.graph.Graph;

/**
 * This class manages the overall workflow 
 * of holistic ontology mapping.
 * @author max
 *
 */
public class HolomaProcessControl {

	public static void main(String[] args) {
		StopWatch stopWatch = new StopWatch();
		
		System.out.println("HolOMa (Holistic Ontology Mapping)\n"
				+ "-------------------------------------------------------------");
		// TODO: file location the property file
		String edgeFileLocation = "/home/max/git/HolOMa/holoma/src/main/resources/example_edges.csv";
		String vertexFileLocation = "/home/max/git/HolOMa/holoma/src/main/resources/example_vertices.csv";
		
		System.out.println("Creating the graph ... ");
		stopWatch.start();
		GraphCreationPoint creationPoint = new GraphCreationPoint(edgeFileLocation, vertexFileLocation);
		Graph<String, String, Integer> graph = creationPoint.createGraph();
		stopWatch.stop(); System.out.println("\t in "+stopWatch.getTime()+"ms");
	
		System.out.println("Evaluating the graph ... ");
		GraphEvaluationPoint eval = new GraphEvaluationPoint(graph);
		stopWatch.start();
		System.out.println("Valiation: "+eval.validateGraph());
		stopWatch.stop(); System.out.println("\t in "+stopWatch.getTime()+"ms");
		stopWatch.start();
		System.out.println("#vertices: "+eval.getCountVertices());
		stopWatch.stop(); System.out.println("\t in "+stopWatch.getTime()+"ms");
		stopWatch.start();
		System.out.println("#edges: "+eval.getCountEdges());
		stopWatch.stop(); System.out.println("\t in "+stopWatch.getTime()+"ms");
		
		System.out.println("--- End ---");
	}

}