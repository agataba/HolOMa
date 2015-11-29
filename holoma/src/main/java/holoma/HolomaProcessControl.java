package holoma;
import java.util.Map;
import java.util.Set;

//
import org.apache.commons.lang.time.StopWatch;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;

import tools.io.InputFromConsole;

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
	
	/** Where to print or where to get the set of edges of the graph. */
	private static final String edgeFileLoc = "./src/main/resources/edges.csv";
	/** Where to print or where to get the set of vertices of the graph. */
	private static final String vertexFileLoc = "./src/main/resources/vertices.csv";
	/** Where to print the connected components. */
	private static final String conCompFileLoc = "./src/main/resources/connectedComponents.csv";
	/** Path of the ontology files. */
	private static final String ontologyPath = "./src/main/resources/ont/";
	/** Name of the mapping file (same path as ontology files). */
	private static final String mappingFile = "mapping_mod.csv";
	/** Names of the ontology files. */
	private static final String[] ontologyFiles = {
/*		"chebi.owljsonLD.json",
		"fma.owljsonLD.json",
		"full-galen.owljsonLD.json",
		"MESH.ttljsonLD.json",
		"NCITNCBO.ttljsonLD.json",
		"NPOntology01.owljsonLD.json",
		"OMIM.ttljsonLD.json",
*/		"PDQ.ttljsonLD.json",
		"Radlex_3.12.owljsonLD.json",
		"RXNORM.ttljsonLD.json"
	};
	/** Specifies whether the preprocessor is optimistic: 'true' for optimistic, 'false' for pessimistic.
	 * An optimistic preprocessor adds missing edges' vertices to the set of all vertices.
	 * A pessimistic preprocessor deletes all edges where at least on vertex is not part of the set of all vertices. */
	private static final boolean isOptimPrepr = false;
	/** Maximum number of iteration steps for connected components. */
	private static final int maxIterations = 10;
	
	private static final boolean noSingletonComponents = true;
	
	
	
	//##################################################################
	//############### main #############################################
	//##################################################################
	public static void main(String[] args) {	
		// #0: Showing settings
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
		
		// #1: Creating the graph
		GraphCreationPoint creation = new GraphCreationPoint();
		Graph<String, String, Integer> graph = null;
		
		while (true) {
			char c = InputFromConsole.readChar("Load graph from ontology files ('o') or from existing edge and vertex file ('e')? \n >> ");
			if (c=='o') { 
				startTime();
				graph = creation.getGraphFromOntologyFiles(ontologyPath, ontologyFiles, mappingFile, isOptimPrepr, edgeFileLoc, vertexFileLoc);
				printTime(); break;
			}
			if (c=='e') {
				startTime();
				graph = creation.getGraphFromEdgeVertexFile(edgeFileLoc, vertexFileLoc);
				printTime(); break;
			}
		}
		
/*		System.out.println("\n!!!!Quit program!!!!");System.exit(0);
*/		
		
		// #2: Evaluating the graph
		// calculate connected components
		System.out.println("\nCalculating Connected Components ... ");
		startTime();
		Map<Long, Set<String>> connCompts = null;
		try {
			startTime();
			connCompts = calculateConnComponents(graph);
			printTime();
		} catch (Exception e){
			System.err.println("Error while calculating connected components.");
			e.printStackTrace();
			System.out.println("\nQuit program ... ");
			System.exit(1);
		}
		// save connected components
		System.out.println("printing to "+conCompFileLoc+" ... ");
		GraphVisualisation.printConnectedComponents(connCompts, conCompFileLoc);
		// analyse connected components: avg/ min/ max size
		startTime();
		System.out.println("Analysing connected components ... ");
		GraphEvaluationPoint.analyseConnComponents(connCompts);
		printTime();
		
		System.out.println("\n--- End ---");
	}
	
	
	
	
	/**
	 * Calculates the connected components and changes their format to a map
	 * which has the component ID as key, and the nodes of this ID as value.
	 * @param graph The components are based on the graph.
	 * @return Connected Components.
	 */
	private static Map<Long, Set<String>> calculateConnComponents (Graph<String, String, Integer> graph){
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