package holoma;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.apache.flink.graph.Graph;

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
	 * @param connCompts Map of connected components.
	 */
	public static void showConnectedComponents (Map<Long, Set<String>> connCompts) {			
		for (Long key : connCompts.keySet()) {
			System.out.println("component ID: "+key);
			System.out.println(connCompts.get(key));
		}		
	}
	
	/**
	 * Prints the connected components to <code>path</code>.
	 * Schema: Each set of connected components is introduced by a dotted line 
	 * followed by the component ID. The following lines contain the particular vertex URLs.
	 * @param connCompts Map of connected components.
	 */
	public static void printConnectedComponents (Map<Long, Set<String>> connCompts) {
		OutputToFile out = new OutputToFile (500, HolomaConstants.CONNCOMP_FILE_LOC);
		for (Long key : connCompts.keySet()) {
			out.addToBuff("-------------\ncomponent ID: "+key);
			for (String vertex : connCompts.get(key))
				out.addToBuff(vertex);
		}	
		out.close();
	}
	
	
	
	

}
