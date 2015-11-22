package holoma;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

import tools.io.OutputToFile;

@SuppressWarnings("serial")
public class GraphVisualisation implements Serializable {

	
	
	@SuppressWarnings("rawtypes")
	public static void showEdgesVertices (Graph g) {
		try {
			g.getEdges().print();
			g.getVertices().print();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
	
	public static void showConnectedComponents (Map<Long, List<String>> conCompts) {			
		for (Long key : conCompts.keySet()) {
			System.out.println("component ID: "+key);
			System.out.println(conCompts.get(key));
		}		
	}
	
	public static void printConnectedComponents (Map<Long, List<String>> conCompts, String path) {
		OutputToFile out = new OutputToFile (500, path);
		for (Long key : conCompts.keySet()) {
			out.addToBuff("-------------\ncomponent ID: "+key);
			out.addToBuff(conCompts.get(key));
		}	
		out.close();
	}
	
	
	public static Map<Long, List<String>> sortConnectedComponents (DataSet<Vertex<String, Long>> verticesWithComponents, boolean noSingletons) {
		Map<Long, List<String>> conCompts = new HashMap<Long, List<String>>();
		try {
			for (Vertex<String, Long> vertex : verticesWithComponents.collect()) {
				// component has already occurred: add entry to existing hash map
				if (conCompts.keySet().contains(vertex.f1))
					conCompts.get(vertex.f1).add(vertex.f0);
				// component is new: create a new entry in the hash map
				else {
					List<String> l = new ArrayList<String>();
					l.add(vertex.f0);
					conCompts.put(vertex.f1, l);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if (noSingletons)
			conCompts = eliminateSingletons (conCompts);
		
		return conCompts;
	}
	
	
	private static Map<Long, List<String>> eliminateSingletons (Map<Long, List<String>> conCompts) {
		Map<Long, List<String>> newMap = new HashMap<Long, List<String>>();
		for (Long key : conCompts.keySet()) {
			if (conCompts.get(key).size() > 1)
				newMap.put(key, conCompts.get(key));
		}
		
		return newMap;		
	}

}
