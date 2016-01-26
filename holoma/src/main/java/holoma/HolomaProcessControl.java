package holoma;
import java.util.Map;
import java.util.Set;

//
import org.apache.commons.lang.time.StopWatch;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.log4j.Logger;

import holoma.complexDatatypes.VertexValue;
import holoma.graph.GraphCreationPoint;
import holoma.graph.GraphVisualisation;
import tools.io.InputFromConsole;
import tools.io.OutputToFile;

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

	
	/** Context in which the program is currently executed. */
	static final ExecutionEnvironment ENV = ExecutionEnvironment.getExecutionEnvironment();
	
	
	//##################################################################
	//############### main #############################################
	//##################################################################
	public static void main(String[] args) {
		
		// #0: Showing settings
		System.out.println("HolOMa (Holistic Ontology Mapping)");
		System.out.println("-------------------------------------------------------------");		
		System.out.println("Properties:");
		System.out.println("edge file location:            "+HolomaConstants.EDGE_FILE_LOC);
		System.out.println("vertex file location:          "+HolomaConstants.VERTEX_FILE_LOC);
		System.out.println("connected components location: "+HolomaConstants.CONNCOMP_FILE_LOC);
		System.out.println("analysis of conn comp location:"+HolomaConstants.ANALYSIS_1_FILE_LOC);
		System.out.println("optimistic preprocessor:       "+HolomaConstants.IS_OPTIM_PREPR);
		System.out.println("printing invalid edges:        "+HolomaConstants.IS_PRINTING_INVALID_EDG);
		System.out.println("printing valid edges/ vertices:"+HolomaConstants.IS_PRINTING_VALID_EDGVERT);
		System.out.println("max. iterations:               "+HolomaConstants.MAX_ITER);
		System.out.println("no singleton components:       "+HolomaConstants.NO_SINGLETON_CONNCOMP);
		System.out.println("-------------------------------------------------------------\n");
		
		startTime();
		
		
		// #1: Creating the graph
		GraphCreationPoint creation = new GraphCreationPoint(ENV);
		Graph<String, String, Integer> graph = null;
		
		while (true) {
			char c = InputFromConsole.readChar("Load graph from ontology files ('o') or from existing edge and vertex file ('e')? \n >> ");
			if (c=='o') { 
				graph = creation.getGraphFromOntologyFiles();
				break;
			}
			if (c=='e') {
				graph = creation.getGraphFromEdgeVertexFile();
				break;
			}
		}
		
		// #2 Calculating connected components
		System.out.println("\nCalculating Connected Components ... ");
		ConnCompCalculation connCompCalc = new ConnCompCalculation(graph);
		Map<Long, Set<String>> connCompts = connCompCalc.calculateConnComp_naive(); 		
		if (connCompts==null || connCompts.size()==0) {
			System.out.println("No connected components.");
			System.out.println("\n--- End ---");
			System.exit(0);
		}		
		// save connected components
		System.out.println("printing to "+HolomaConstants.CONNCOMP_FILE_LOC+" ... ");
		GraphVisualisation.printConnectedComponents(connCompts);
		
		// #3: Analyzing connected components
		System.out.println("\nAnalysing connected components ... ");
		String analysisResult = connCompCalc.analyseConnComponents();
		OutputToFile out = new OutputToFile(100, HolomaConstants.ANALYSIS_1_FILE_LOC);
		out.addToBuff(analysisResult); out.close();		
		
		// #4: Enriching connected components
		System.out.println("\nEnriching connected components ... ");
		ConnCompEnrichment enr = 
				new ConnCompEnrichment(HolomaConstants.ENR_DEPTH, graph, HolomaConstants.MAP_WEIGHT, ENV);
		String vis = "";
		for (long key : connCompts.keySet()) { 
			Set<String> connComp = connCompts.get(key);
			int connComptSize = connComp.size();
			// check whether component has critical size
			if (connComptSize >= HolomaConstants.MIN_CC_SIZE) {
				Graph<String, VertexValue, Float> g = enr.getEnrichedConnComp(connComp);
				// print enriched connected components
/*				String path = "./src/main/resources/output/";
				System.out.println("printing to \t"+path+key+"_edges.txt \n\t and \t"+path+key+"_vertices.txt");
				GraphVisualisation.printGraph(g, path+key+"_edges.txt", path+key+"_vertices.txt");
*/				vis += "----------\n"+GraphVisualisation.showEdgesVertices(g);
				
			}
		}
		
		System.out.println(vis);
		
		
		
		printTime();
		System.out.println("\n--- End ---");
	}
	
	
	
	
	
	
	
	//############### time measure methods ###############
	
	/** Starts the stop watch. */
	private static void startTime () { stopWatch.reset(); stopWatch.start(); }
	
	/** Prints the time since starting the stop watch to the console. */
	private static void printTime () {
		stopWatch.stop();
		System.out.println("\t in "+stopWatch.getTime()+"ms");
	}
	

}