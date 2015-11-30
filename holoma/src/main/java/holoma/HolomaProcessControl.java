package holoma;
import java.util.ArrayList;
import java.util.HashMap;
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
	private static final String mappingFile = "mapping_mod.csv"; //"mapping_color.csv"; 
	/** Names of the ontology files. */
	private static final String[] ontologyFiles = {
/*		"blue.ttljsonLD.json",
		"green.ttljsonLD.json",
		"orange.ttljsonLD.json"
*/		"chebi.owljsonLD.json",
		"fma.owljsonLD.json",
		"full-galen.owljsonLD.json",
		"MESH.ttljsonLD.json",
		"NCITNCBO.ttljsonLD.json",
		"NPOntology01.owljsonLD.json",
		"OMIM.ttljsonLD.json",
		"PDQ.ttljsonLD.json",
		"Radlex_3.12.owljsonLD.json",
		"RXNORM.ttljsonLD.json"
	};
	
	/** Specifies whether the preprocessor is optimistic: 'true' for optimistic, 'false' for pessimistic.
	 * An optimistic preprocessor adds missing edges' vertices to the set of all vertices.
	 * A pessimistic preprocessor deletes all edges where at least on vertex is not part of the set of all vertices. */
	private static final boolean isOptimPrepr = true;
	/** Invalid edges are printed iff 'true'.
	 *  Invalid edges are calculated iff <code>isOptimPrepr=false</code>. */
	private static final boolean isPrintingInvalEdges = false;
	/** Valid edges and vertices are printed iff 'true'. */
	private static final boolean isPrintingValidEdgVert = false;
	/** Maximum number of iteration steps for connected components. */
	private static final int maxIterations = 10;
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
		
		
		
		Map<Long, Set<String>> connCompts = null;
		
		while (true) {
			char c = InputFromConsole.readChar("Naive ('n') or dynamic ('d')? \n >> ");
			if (c=='n') {
				connCompts = doNaive(); break;
			}
			else if (c=='d') {
				connCompts = doDynamic(); break;
			}
		}
		if (connCompts==null || connCompts.size()==0) {
			System.out.println("No connected components.");
			System.out.println("\n--- End ---");
			System.exit(0);
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
	
	
	
	private static Map<Long, Set<String>> doNaive () {
		// #1: Creating the graph
		GraphCreationPoint creation = new GraphCreationPoint(isPrintingValidEdgVert);
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
		
		return connCompts;				
	}
	
	
	private static Map<Long, Set<String>> doDynamic() {
		return calculateConnComponentsDynamic();
	}
	
	
	/**
	 * Calculates the connected components and changes their format to a map
	 * which has the component ID as key, and the nodes of this ID as value.
	 * @param graph The components are based on the graph.
	 * @return Connected Components.
	 */
	private static Map<Long, Set<String>> calculateConnComponents (Graph<String, String, Integer> graph){
		// exactly one ontology: no connected components
		if (ontologyFiles.length<=1) return null;
		GraphEvaluationPoint eval = new GraphEvaluationPoint(graph, maxIterations);
		DataSet<Vertex<String, Long>> verticesWithComponents = eval.getConnectedComponents();
			
		return GraphVisualisation.sortConnectedComponents(verticesWithComponents, noSingletonComponents);
	}
	
	
	private static Map<Long, Set<String>> calculateConnComponentsDynamic () {
		// #1: determine connected components between two ontologies
		// e.g.: Ontologies = {O, P, Q} then connComp(O,P) and connComp(P,Q)
		List<Map<Long, Set<String>>> listConnCompts = new ArrayList<Map<Long, Set<String>>>(ontologyFiles.length-1);
		for (int i=0; i<ontologyFiles.length-1; i++) {
			String[] ontologies = {ontologyFiles[i], ontologyFiles[i+1]};
			String edgeFile = edgeFileLoc.substring(0, edgeFileLoc.lastIndexOf('.'))+"_"+i+".csv";
			String vertexFile = vertexFileLoc.substring(0, vertexFileLoc.lastIndexOf('.'))+"_"+i+".csv";
			// create graph for two ontologies
			GraphCreationPoint creation = new GraphCreationPoint(isPrintingValidEdgVert);
			Graph<String, String, Integer> graph = 
					creation.getGraphFromOntologyFiles(ontologyPath, ontologies, mappingFile, isOptimPrepr, edgeFile, vertexFile);
			
			// calculate connected components on that graph
			GraphEvaluationPoint eval = new GraphEvaluationPoint(graph, maxIterations);
			DataSet<Vertex<String, Long>> verticesWithComponents = eval.getConnectedComponents();
			// add the connected components to the list
			Map<Long, Set<String>> connCompts = GraphVisualisation.sortConnectedComponents(verticesWithComponents, noSingletonComponents);
/*			GraphVisualisation.showEdgesVertices(graph);
			GraphVisualisation.showConnectedComponents(connCompts);
			InputFromConsole.readLine();
*/			listConnCompts.add(connCompts);
		}
		
		// #2: combine two maps of connected components iff they share at least one vertex
		// e.g.: connComp(O,P) = { 1->{O2,P1}, 2->{O6,P7} }, connComp(P,Q) = { 3->{P6,Q3}, 4->{P7,Q5} }
		//       then: connComp(O,P,Q) = { 1->... , 2->{O6,P7,Q5}, 3->... }
		while (listConnCompts.size() > 1) {
/*			System.out.println("\nCombining connected components (list size: "+listConnCompts.size()+") ... ");
*/			List<Map<Long, Set<String>>> prevListConnCompts = listConnCompts;
/*			System.out.println("list: "+listConnCompts);
			System.out.println("size: "+prevListConnCompts.size());
*/			listConnCompts = new ArrayList<Map<Long, Set<String>>>();
			
			// compare two "neighbouring" maps of connected components
			// if their connected components share at least one element, then combine these sets to a new one
			for (int i=0; i<prevListConnCompts.size()-1; i++) {
/*				System.out.println("  ... between map "+i+" and "+(i+1));
*/				Map<Long, Set<String>> map1 = prevListConnCompts.get(i);
				Map<Long, Set<String>> map2 = prevListConnCompts.get(i+1);				
				Map<Long, Set<String>> mapNew = combineMaps(map1, map2);
				
				listConnCompts.add(mapNew);
				
			}
		}
		
		return listConnCompts.get(0);		
	}
	
	/**
	 * Combines two maps of connected components. Sets of connected components which share at least
	 * one vertex are combined (because they represent a "bigger" connected component). Sets which
	 * do not share any element with a set of the other map are copied without changes to output.
	 * @param map1 A map from component ID to set of connected components.
	 * @param map2 Another map from component ID to set of connected components.
	 * @return Combined map of connected components.
	 */
	private static Map<Long, Set<String>> combineMaps (Map<Long, Set<String>> map1, Map<Long, Set<String>> map2) {
/*		System.out.println("comineMaps with map1: "+map1+" and map2: "+map2);
*/		Map<Long, Set<String>> mapNew = new HashMap<Long, Set<String>>();
		
		// maps for managing whether a component set is combined with another one
		Map<Long, Boolean> map1IsCombined = new HashMap<Long, Boolean>();
		Map<Long, Boolean> map2IsCombined = new HashMap<Long, Boolean>();
		for (Long key1 : map1.keySet()) map1IsCombined.put(key1, false);
		for (Long key2 : map2.keySet()) map2IsCombined.put(key2, false);
		
		long mapKey = 0;
		for (Long key1 : map1.keySet()) {
			Set<String> set1 = map1.get(key1);
			for (Long key2 : map2.keySet()) {
				Set<String> set2 = map2.get(key2);
				if (set1.removeAll(set2)) {
					// set1 and set2 share at least one element
					// combine the two sets
					Set<String> setNew = set1;
					setNew.addAll(set2);
					map1IsCombined.put(key1, true);
					map2IsCombined.put(key2, true);
					mapNew.put(mapKey, setNew);
					mapKey++;
				}						
			}
		}
		// add connected components which do not share any element to output
		for (Long key1 : map1.keySet())
			if (!map1IsCombined.get(key1)) { 
				mapNew.put(mapKey, map1.get(key1));
				mapKey++;
			}		
		for (Long key2 : map2.keySet())
			if (!map2IsCombined.get(key2)) { 
				mapNew.put(mapKey, map2.get(key2));
				mapKey++;
			}
		
		return mapNew;
	}
	
	
	
	
	/** Starts the stop watch. */
	private static void startTime () { stopWatch.reset(); stopWatch.start(); }
	
	/** Prints the time since starting the stop watch to the console. */
	private static void printTime () {
		stopWatch.stop();
		System.out.println("\t in "+stopWatch.getTime()+"ms");
	}
	

}