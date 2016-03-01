package holoma;
import holoma.complexDatatypes.EdgeValue;
import holoma.complexDatatypes.Vertex2RankMap;
import holoma.complexDatatypes.VertexValue;
import holoma.connComp.ConnCompCalculation;
import holoma.connComp.ConnCompEnrichment;
import holoma.connComp.ConnectedComponentSizeFilter;
import holoma.graph.GraphCreationPoint;
import holoma.parsing.Dictionary;
import holoma.ppr.OverallSumCalculator;
import holoma.ppr.PersonalizedPageRank;
import holoma.ppr.PersonalizedPageRankPerVertex;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;



























































//
import org.apache.commons.lang.time.StopWatch;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeJoinFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

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

	
	
	/** Context in which the program is currently executed. */
	
	
	//##################################################################
	//############### main #############################################
	//##################################################################
	public static void main(String[] args) {
		ExecutionEnvironment ENV;
		//ENV= ExecutionEnvironment.getExecutionEnvironment();
		 Configuration conf = new Configuration();
		  conf.setLong("taskmanager.network.numberOfBuffers", 60000L);
		  conf.setBoolean("taskmanager.debug.memory.startLogThread", true);
		  conf.setDouble("taskmanager.memory.segment-size", Math.pow(2, 20));
		  conf.setDouble("taskmanager.memory.fraction", 0.9d);
		  //conf.setLong("taskmanager.network.bufferSizeInBytes", 60000L);
		  ENV =  new LocalEnvironment(conf); 
		
		PropertyConfigurator.configure("log4j.properties");
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
		String path = null;
		if (args.length==1){
			path = args[0];
		}
	
		
		
		
		// #1: Creating the graph
		GraphCreationPoint creation = new GraphCreationPoint(ENV);
		Graph<Long, VertexValue, Float> graph;
		
		while (true) {
			char c = InputFromConsole.readChar("Load graph from ontology files ('o') or from existing edge and vertex file ('e')? \n >> ");
			if (c=='o'&&path!=null) { 
				graph = creation.getGraphFromOntologyFiles(path);
				break;
			}
			if (c=='e') {
				graph = creation.getGraphFromEdgeVertexFile();
				break;
			}
		}
	
		// #2 Calculating connected components
		System.out.println("\nCalculating Connected Components ... ");
		
		ConnCompCalculation connCompCalc = new ConnCompCalculation();	
		DataSet<Vertex<Long, Long>> comComptsDataset = connCompCalc.getConnectedComponents(graph);
		//transformation necessary to join with graph vertices
		DataSet<Tuple2<Long,VertexValue>> compDataSet = comComptsDataset.map(new MapFunction<Vertex<Long,Long>,
				
				Tuple2<Long,VertexValue>>(){
					private static final long serialVersionUID = -7970584938311634001L;

					@Override
					public Tuple2<Long, VertexValue> map(
							Vertex<Long, Long> value) throws Exception {
						// TODO Auto-generated method stub
						VertexValue vv = new VertexValue();
						vv.setConComponent(value.getValue());
						return new Tuple2<Long,VertexValue>(value.f0,vv);
					}
			
		});
	
		// join with vertices to add the set of component ids for each vertex
		graph = graph.joinWithVertices(compDataSet, new VertexJoinFunction<VertexValue, VertexValue>(){

			private static final long serialVersionUID = 1L;

			@Override
			public VertexValue vertexJoin(VertexValue vertexValue,
					VertexValue inputValue) throws Exception {
				// TODO Auto-generated method stub
				vertexValue.addCompId(inputValue.getConComponent());
				return vertexValue;
			}
			
		});
		
		//propagate the component ids of a vertex in a component to the vertices in the neighborhood 
		ConnCompEnrichment enr = new ConnCompEnrichment();
		//vertex centric iteration 
//		Map<Long,Set<Long>>components = new HashMap<Long,Set<Long>>();
		graph= enr.enrichConnectedComponent(HolomaConstants.ENR_DEPTH, graph);	
		graph = ConnectedComponentSizeFilter.filterGraphByComponentSize(graph, HolomaConstants.MIN_CC_SIZE, HolomaConstants.MAX_CC_SIZE);

//		
//		try {
//			log.info("vertices:"+graph.getVertices().count()+" edges:"+ graph.getEdges().count());
//		} catch (Exception e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//		
		
		startTime();
		Graph <Long,VertexValue,Map<Long,Float>>sumReducedGraph = graph.mapEdges(new MapFunction<Edge<Long,Float>,Map<Long,Float>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 6558208271233248698L;

			@Override
			public Map<Long, Float> map(Edge<Long, Float> value)
					throws Exception {
				// TODO Auto-generated method stub
				Map<Long,Float> v =new HashMap<Long,Float>();
				v.put(1l, value.f2);
				return v;
			}
			
		});
		
		OverallSumCalculator sum = new OverallSumCalculator();
		DataSet<Tuple2<Long,Map<Long,Float>>> is_aSum = sum.getOverallSumDataSet(false, graph);
		sumReducedGraph = sumReducedGraph.joinWithEdgesOnSource(is_aSum, new EdgeJoinFunction<Map<Long,Float>,Map<Long,Float>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1768058051277440628L;
			@Override
			public Map<Long, Float> edgeJoin(Map<Long,Float> edgeValue,
					Map<Long, Float> inputValue) throws Exception {
				Map<Long,Float> v =new HashMap<Long,Float>();
				for (Long cid: inputValue.keySet()){
					v.put(cid, edgeValue.get(1l)/inputValue.get(cid));
				}
				return v;
			}

			
		});
		
		PersonalizedPageRank pageRank = new PersonalizedPageRank();
		DataSet<Tuple4<Long,Long,Long,Float>> result = pageRank.calculatePPrForEachCompAndSource(sumReducedGraph);
		Map<Long,Map<Long,Vertex2RankMap>> resultMap = new HashMap<Long,Map<Long,Vertex2RankMap>>();
		try {
			List<Tuple4<Long,Long,Long,Float>> list = result.collect();
			for (Tuple4<Long,Long,Long,Float> t : list){
				Map<Long,Vertex2RankMap> vMap = resultMap.get(t.f0);
				if (vMap ==null){
					 vMap = new HashMap<Long,Vertex2RankMap>();
					resultMap.put(t.f0, vMap);
				}
				Vertex2RankMap verMap = vMap.get(t.f2);
				if (verMap==null){
					verMap = new Vertex2RankMap();
					vMap.put(t.f2, verMap);
				}
				verMap.addRankForVertex(t.f1, t.f3);
			}
			Map <Long,Vertex2RankMap> largestMap = new HashMap<Long,Vertex2RankMap>();
			for (Entry<Long,Map<Long,Vertex2RankMap>>e:resultMap.entrySet()){
				if (e.getValue().size()>largestMap.size()){
					largestMap = e.getValue();
				}
//				for (Entry<Long,Vertex2RankMap>v: e.getValue().entrySet()){
//					StringBuffer sb = new StringBuffer ();
//					for (Entry<Long,Float> vr : v.getValue().getRankings().entrySet()){
//						sb.append(Dictionary.getInstance().getReverse(vr.getKey())+"="+vr.getValue()+",");
//					}
//					log.info(Dictionary.getInstance().getReverse(v.getKey())+" ranking: {"+sb.toString()+"}");
//				}
			}
			printTime();
//			log.info(largestMap.size());
//			for (Entry<Long,Vertex2RankMap>v: largestMap.entrySet()){
//				StringBuffer sb = new StringBuffer ();
//				for (Entry<Long,Float> vr : v.getValue().getRankings().entrySet()){
//					sb.append(Dictionary.getInstance().getReverse(vr.getKey())+"="+vr.getValue()+",");
//				}
//				log.info(Dictionary.getInstance().getReverse(v.getKey())+" ranking: {"+sb.toString()+"}");
//			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
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