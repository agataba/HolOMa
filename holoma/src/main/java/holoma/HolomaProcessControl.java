package holoma;
import java.util.List;
import java.util.Map;
import java.util.Set;

//
import org.apache.commons.lang.time.StopWatch;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;

import holoma.parsing.ParsingPoint;

/**
 * This class manages the overall workflow 
 * of holistic ontology mapping.
 * @author max
 *
 */
public class HolomaProcessControl {
	
	/** Log4j message logger. */
	static Logger log = Logger.getLogger("HolomaProcessControl");
	
	/** Stop watch. */
	private static final StopWatch stopWatch = new StopWatch();
	
	// TODO: file location the property file
	/*	private static final String edgeFileLoc = "./src/main/resources/example_edges.csv";
		private static final String vertexFileLoc = "./src/main/resources/example_vertices.csv";
		private static final String conCompFileLoc = "./src/main/resources/example_connectedComponents.csv";
	*/	
	
	private static final String edgeFileLoc = "./src/main/resources/edges.csv";
	private static final String vertexFileLoc = "./src/main/resources/vertices.csv";
	
	private static final String conCompFileLoc = "./src/main/resources/connectedComponents.csv";
	
	private static final String ontologyPath = "./src/main/resources/ont/";
	private static final String mappingFile = "mapping_mod.csv";
	private static final String[] ontologyFiles = {
		"chebi.owljsonLD.json",
/*		"fma.owljsonLD.json",
		"full-galen.owljsonLD.json",
		"MESH.ttljsonLD.json",
		"NCITNCBO.ttljsonLD.json",
*/		"NPOntology01.owljsonLD.json",
		"OMIM.ttljsonLD.json",
		"PDQ.ttljsonLD.json",
		"Radlex_3.12.owljsonLD.json",
		"RXNORM.ttljsonLD.json"
	};

	private static final boolean isOptimPrepr = false;
	
	private static final int maxIterations = 10;
	private static final boolean noSingletonComponents = true;
	
	
	
	//##################################################################
	//############### main #############################################
	//##################################################################
	public static void main(String[] args) {	
		System.out.println("HolOMa (Holistic Ontology Mapping)\n"
				+ "-------------------------------------------------------------");		
		System.out.println("Properties:");
		System.out.println("edge file location:            "+edgeFileLoc);
		System.out.println("vertex file location:          "+vertexFileLoc);
		System.out.println("connected components location: "+conCompFileLoc);
		System.out.println("optimistic preprocessor:       "+isOptimPrepr);
		System.out.println("max. iterations:               "+maxIterations);
		System.out.println("no singleton components:       "+noSingletonComponents);
		System.out.println("-------------------------------------------------------------\n");
		
		
		// #0: Load vertices and edges	 
		startTime();
		ParsingPoint pp = new ParsingPoint (ontologyPath, ontologyFiles, mappingFile, isOptimPrepr);		
		pp.printEdgeVertexToFile(edgeFileLoc, vertexFileLoc);
		Set<Edge<String, Integer>> edges = pp.getEdges();
		Set<Vertex<String, String>> vertices = pp.getVertices();
		System.out.println();
		System.out.println("Printing "+edges.size()+" edges to file  ... ");
		System.out.println("Printing "+vertices.size()+" vertices to file ... ");
		printTime();		
				
		 // #1: Creating the graph		 
		System.out.println("\nCreating the graph ... ");
		startTime();
		GraphCreationPoint creationPoint = new GraphCreationPoint();
		Graph<String, String, Integer> graph = creationPoint.createGraph(edges, vertices);
		printTime();
		
/*		System.out.println("\n!!!!Quit program!!!!");System.exit(0);
		
		System.out.println("Showing the graph ... ");
		GraphVisualisation.showEdgesVertices(graph);	
*/		
		// #2: Evaluating the graph
		
		System.out.println("\nEvaluating the graph ... ");		
		System.out.println("\nCalculating Connected Components ... ");
		startTime();
		Map<Long, List<String>> connCompts = calculateConnComponents(graph);
/*		GraphVisualisation.showConnectedComponents(conCompts);
*/		System.out.println("printing to "+conCompFileLoc+" ... ");
		GraphVisualisation.printConnectedComponents(connCompts, conCompFileLoc);
		printTime();
	
		GraphEvaluationPoint.analyseConnComponents(connCompts);
		
		System.out.println("\n--- End ---");
	}
	
	/**
	 * Calculates the connected components and changes their format to a map
	 * which has the component ID as key, and the nodes of this ID as value.
	 * @param graph The components are based on the graph.
	 * @return Connected Components.
	 */
	private static Map<Long, List<String>> calculateConnComponents (Graph<String, String, Integer> graph){
		GraphEvaluationPoint eval = new GraphEvaluationPoint(graph, maxIterations);
		DataSet<Vertex<String, Long>> verticesWithComponents = eval.getConnectedComponents();
			
		return GraphVisualisation.sortConnectedComponents(verticesWithComponents, noSingletonComponents);
	}
	
	
	
	
	/** Starts the stop watch. */
	private static void startTime () { stopWatch.reset(); stopWatch.start(); }
	
	/** Prints the time since starting the stop watch to the console. */
	private static void printTime () {
		stopWatch.stop();
		System.out.println("\t in "+stopWatch.getTime()+"ms");
	}
	

}