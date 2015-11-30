package holoma.parsing;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

import tools.io.OutputToFile;

/**
 * Manages the correctness of the parsed data.
 * @author max
 *
 */
public class Preprocessor {
	
	public static boolean isPrintingInvalEdges = true;
	
	/**
	 * Adds the missing vertices from the edge set <code>edges</code> to the set <code>vertices</code>.
	 * This assumes that the set of edges is valid and each vertex of the edges
	 * has to be defined in the vertex set.
	 * @param vertices Set of vertices.
	 * @param edges Set of edges.
	 * @param ontName Abbreviated name of the ontology.
	 * @return Expanded and thus valid set of vertices.
	 */
	public static Set<Vertex<String, String>> addMissingVertices (Set<Vertex<String, String>> vertices, Set<Edge<String, Integer>> edges, String ontName) {
		Set<Vertex<String, String>> validVertices = new HashSet<Vertex<String, String>>();
		validVertices.addAll(vertices);
		
		Set<String> vertexNames = new HashSet<String>();
		for (Vertex<String, String> v : vertices)
			vertexNames.add(v.getId());
	
		for (Edge<String, Integer> e : edges) {
			String srcName = e.getSource();
			String trgName = e.getTarget();
			if (! vertexNames.contains(srcName)) {
				Vertex<String, String> v = new Vertex<String, String>(srcName, ontName);
				validVertices.add(v);
			}
			if (! vertexNames.contains(trgName)) {
				Vertex<String, String> v = new Vertex<String, String>(trgName, ontName);
				validVertices.add(v);
			}
		}		
		return validVertices;
	}
	
	
	/**
	 * Removes all invalid edges from the edge set <code>edges</code>.
	 * An edge is invalid iff at least one of its vertices is not part of <code>vertices</code>.
	 * @param vertices Set of vertices.
	 * @param edges Set of edges.
	 * @param ontName Name of the ontology.
	 * @return Set of valid edges.
	 */
	public static Set<Edge<String, Integer>> removeInvalidEdges (Set<Vertex<String, String>> vertices, 
			Set<Edge<String, Integer>> edges, String ontName) {
		Set<Edge<String, Integer>> validEdges = new HashSet<Edge<String, Integer>>();
		// a vertex' name is its identifier, thus the set of vertex names is calculated
		Set<String> vertexNames = new HashSet<String>();
		for (Vertex<String, String> v : vertices)
			vertexNames.add(v.getId());
		
		OutputToFile out = null;
		if (isPrintingInvalEdges)
			out = new OutputToFile (500, "./src/main/resources/invalidEdges_"+ontName+".csv");
		
		for (Edge<String, Integer> e : edges) {
			String srcName = e.getSource();
			String trgName = e.getTarget();
			if (vertexNames.contains(srcName) && vertexNames.contains(trgName))
				validEdges.add(e);
			else {
				if (isPrintingInvalEdges)
					out.addToBuff(srcName+"\t"+trgName+"\t"+e.getValue());	
			}
		}		
		if (isPrintingInvalEdges) {
			out.close();
			System.out.println("Printing invalid edge file to ./src/main/resources/invalidEdges_"+ontName+".csv");
		}		
		return validEdges;
	}
	
	

}
