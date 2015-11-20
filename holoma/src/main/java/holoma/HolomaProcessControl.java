package holoma;

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
		String edgeFileLocation = "/home/max/workspaceJava/holoma/src/main/resources/example_edges.csv";
		String vertexFileLocation = "/home/max/workspaceJava/holoma/src/main/resources/example_vertices.csv";
		
		System.out.println("Creating the graph ... ");
		stopWatch.start();
		GraphCreationPoint creationPoint = new GraphCreationPoint(edgeFileLocation, vertexFileLocation);
		Graph<String, String, String> graph = creationPoint.createGraph();
		stopWatch.stop(); System.out.println("\t in "+stopWatch.getTime()+"ms");
		
		System.out.println("Evaluating the graph ... ");
		GraphEvaluationPoint eval = new GraphEvaluationPoint(graph);
		System.out.println("#vertices: "+eval.getCountVertices());
		System.out.println("#edges: "+eval.getCountEdges());
		
		System.out.println("--- End ---");
	}

}