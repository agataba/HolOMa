package holoma;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

//
import org.apache.commons.lang.time.StopWatch;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;

import holoma.parsing.Preprocessor;
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
	private static final String mappingFile = "mapping_color.csv"; //"mapping.csv"; 
	/** Names of the ontology files. */
	private static final String[] ontologyFiles = {
		"blue.ttljsonLD.json",
		"green.ttljsonLD.json",
		"orange.ttljsonLD.json"
/*			"RXNORM.ttljsonLD.json",
		"PDQ.ttljsonLD.json",
		"NPOntology01.owljsonLD.json"/*,
		"full-galen.owljsonLD.json",
		"MESH.ttljsonLD.json",
		"OMIM.ttljsonLD.json",
		"Radlex_3.12.owljsonLD.json",
		"chebi.owljsonLD.json",
		"fma.owljsonLD.json",
		"NCITNCBO.ttljsonLD.json"
*/	};
	
	/** Specifies whether the preprocessor is optimistic: 'true' for optimistic, 'false' for pessimistic.
	 * An optimistic preprocessor adds missing edges' vertices to the set of all vertices.
	 * A pessimistic preprocessor deletes all edges where at least on vertex is not part of the set of all vertices. 
	 * Blank nodes are always deleted. */
	private static final boolean isOptimPrepr = true;
	/** Invalid edges are printed iff 'true'.
	 *  Invalid edges are calculated iff <code>isOptimPrepr=false</code>. */
	private static final boolean isPrintingInvalEdges = false;
	/** Valid edges and vertices are printed iff 'true'. */
	private static final boolean isPrintingValidEdgVert = true;
	/** Maximum number of iteration steps for connected components. */
	private static final int maxIterations = 2;
	/** Singletons of connected components are eliminated iff 'true'. */
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
		System.out.println("printing invalid edges:        "+isPrintingInvalEdges);
		System.out.println("printing valid edges/ vertices:"+isPrintingValidEdgVert);
		System.out.println("max. iterations:               "+maxIterations);
		System.out.println("no singleton components:       "+noSingletonComponents);
		System.out.println("-------------------------------------------------------------\n");
		
		Preprocessor.isPrintingInvalEdges = isPrintingInvalEdges;
		
		
		// #1: Calculating connected components
		startTime();
		System.out.println("\nCalculating Connected Components ... ");
		Map<Long, Set<String>> connCompts = doNaive(); 		
		if (connCompts==null || connCompts.size()==0) {
			System.out.println("No connected components.");
			System.out.println("\n--- End ---");
			System.exit(0);
		}		
		printTime();		
		// save connected components
		System.out.println("printing to "+conCompFileLoc+" ... ");
		GraphVisualisation.printConnectedComponents(connCompts, conCompFileLoc);
		
		// #2: Analyzing connected components
		startTime();
		System.out.println("\nAnalysing connected components ... ");
		GraphEvaluationPoint.analyseConnComponents(connCompts);
		printTime();
		
		System.out.println("\n--- End ---");
	}
	
	
	
	
	
	/**
	 * Naive approach: calculate connected components on the whole graph of all ontologies.
	 * @return Map from component ID to a connected component, i.e., a set of vertices.
	 */
	private static Map<Long, Set<String>> doNaive () {
		// #1: Creating the graph
		GraphCreationPoint creation = new GraphCreationPoint(isPrintingValidEdgVert);
		Graph<String, String, Integer> graph = null;
		
		while (true) {
			char c = InputFromConsole.readChar("Load graph from ontology files ('o') or from existing edge and vertex file ('e')? \n >> ");
			if (c=='o') { 
				graph = creation.getGraphFromOntologyFiles(ontologyPath, ontologyFiles, mappingFile, isOptimPrepr, edgeFileLoc, vertexFileLoc);
				break;
			}
			if (c=='e') {
				graph = creation.getGraphFromEdgeVertexFile(edgeFileLoc, vertexFileLoc);
				break;
			}
		}
		
		GraphEvaluationPoint eval = new GraphEvaluationPoint(graph, maxIterations);
		
/*		System.out.println("\n!!!!Quit program!!!!");System.exit(0);	
*/		
		// #2: Evaluating the graph
		// calculate connected components
		Map<Long, Set<String>> connCompts = null;
		try {
			// exactly one ontology: no connected components
			connCompts = (ontologyFiles.length<=1) ? null : eval.calculateConnComponents(noSingletonComponents);
		} catch (Exception e){
			System.err.println("Error while calculating connected components.");
			e.printStackTrace();
			System.out.println("\nQuit program ... ");
			System.exit(1);
		}
		
		return connCompts;				
	}
	
	
	
	/** Starts the stop watch. */
	private static void startTime () { stopWatch.reset(); stopWatch.start(); }
	
	/** Prints the time since starting the stop watch to the console. */
	private static void printTime () {
		stopWatch.stop();
		System.out.println("\t in "+stopWatch.getTime()+"ms");
	}
	

}