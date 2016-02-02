package holoma;
import java.util.List;
import java.util.Map;
import java.util.Set;

//
import org.apache.commons.lang.time.StopWatch;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;

import holoma.complexDatatypes.VertexValue;
import holoma.connComp.ConnCompCalculation;
import holoma.connComp.ConnCompEnrichment;
import holoma.graph.GraphCreationPoint;
import holoma.graph.GraphVisualisation;
import holoma.ppr.PersonalizedPageRank;
import holoma.ppr.PersonalizedPageRankEvaluation;
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
		System.out.println("analysis of conn comp location:"+HolomaConstants.ANALYSIS_CC_FILE_LOC);
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
		OutputToFile out = new OutputToFile(100, HolomaConstants.ANALYSIS_CC_FILE_LOC);
		out.addToBuff(analysisResult); out.close();		
		
		// #4: Enriching connected components
		System.out.println("\nEnriching connected components ... ");
		ConnCompEnrichment enr = 
				new ConnCompEnrichment(HolomaConstants.ENR_DEPTH, graph, HolomaConstants.MAP_WEIGHT, ENV);
		PersonalizedPageRank pageRank = new PersonalizedPageRank();
		// iterate over each connected component
		for (long key : connCompts.keySet()) { 
			Set<String> connComp = connCompts.get(key);
			int connComptSize = connComp.size();
			// check whether component has critical size
			if (connComptSize >= HolomaConstants.MIN_CC_SIZE && connComptSize <= HolomaConstants.MAX_CC_SIZE) {
				// get enriched connected component
				Graph<String, VertexValue, Float> enrConnComp = enr.getEnrichedConnComp(connComp);
				try {
					//calculate page rank
					pageRank.setEnrConnComp(enrConnComp);
					pageRank.start();
					Map<String, List<Vertex<String, VertexValue>>> prVectors = pageRank.getMapCalcPageRanks();
					PersonalizedPageRankEvaluation prEval = new PersonalizedPageRankEvaluation();
					prEval.setEvalData(enrConnComp, prVectors);
					System.out.println(prEval.showPrVectors());
				} catch (Exception e) {
					System.err.println("Exception during page rank calculation.");
					e.printStackTrace();
				}				
			} // end 'if' of critical component size
		} // end 'for' of iteration over connected components 
		
		
		
		
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