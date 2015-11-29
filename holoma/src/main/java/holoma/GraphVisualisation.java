package holoma;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

import tools.io.OutputToFile;

/**
 * Contains different methods for visualisation of a graph
 * and its connected components.
 * @author max
 *
 */
@SuppressWarnings("serial")
public class GraphVisualisation implements Serializable {

	
	/**
	 * Prints the edges and vertices of a graph <code>g</code> to the console.
	 * @param g A graph.
	 */
	@SuppressWarnings("rawtypes")
	public static void showEdgesVertices (Graph g) {
		try {
			g.getEdges().print();
			g.getVertices().print();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
	
	/**
	 * Prints the connected components to the console.
	 * @param conCompts Map of connected components.
	 */
	public static void showConnectedComponents (Map<Long, List<String>> conCompts) {			
		for (Long key : conCompts.keySet()) {
			System.out.println("component ID: "+key);
			System.out.println(conCompts.get(key));
		}		
	}
	
	/**
	 * Prints the connected components to <code>path</code>.
	 * Schema: Each set of connected components is introduced by a dotted line 
	 * followed by the component ID. The following lines contain the particular vertex URLs.
	 * @param conCompts Map of connected components.
	 * @param path Where to print the connected components.
	 */
	@SuppressWarnings("unchecked")
	public static void printConnectedComponents (Map<Long, Set<String>> conCompts, String path) {
		OutputToFile out = new OutputToFile (500, path);
		for (Long key : conCompts.keySet()) {
			out.addToBuff("-------------\ncomponent ID: "+key);
			out.addToBuff((List<String>) conCompts.get(key));
		}	
		out.close();
	}
	
	/**
	 * Sorts the nodes according to their association with a connected component. 
	 * All nodes with the same component ID are grouped together. They are mapped
	 * to their component ID.
	 * @param verticesWithComponents Vertices with the connected component ID as value.
	 * @param noSingletons Singletons of connected components are eliminated iff 'true'.
	 * @return Map from component ID to its set of connected vertices.
	 */
	public static Map<Long, Set<String>> sortConnectedComponents (DataSet<Vertex<String, Long>> verticesWithComponents, boolean noSingletons) {
		Map<Long, Set<String>> conCompts = new HashMap<Long, Set<String>>();
		try {
			for (Vertex<String, Long> vertex : verticesWithComponents.collect()) {
				// component has already occurred: add entry to existing hash map
				if (conCompts.keySet().contains(vertex.f1))
					conCompts.get(vertex.f1).add(vertex.f0);
				// component is new: create a new entry in the hash map
				else {
					Set<String> l = new HashSet<String>();
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
	
	/**
	 * Eliminates singletons from the set of connected components.
	 * @param conCompts Set of connected components (potentially components of cardinality one).
	 * @return Set of connected components such that each connected component contains at least two nodes.
	 */
	private static Map<Long, Set<String>> eliminateSingletons (Map<Long, Set<String>> conCompts) {
		Map<Long, Set<String>> newMap = new HashMap<Long, Set<String>>();
		for (Long key : conCompts.keySet()) {
			if (conCompts.get(key).size() > 1)
				newMap.put(key, conCompts.get(key));
		}		
		return newMap;		
	}

}
