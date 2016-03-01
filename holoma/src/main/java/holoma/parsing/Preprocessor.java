package holoma.parsing;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

import holoma.HolomaConstants;
import holoma.complexDatatypes.EdgeValue;
import holoma.complexDatatypes.VertexValue;
import tools.io.OutputToFile;

/**
 * Manages the correctness of the parsed data.
 * @author max
 *
 */
public class Preprocessor {
	
	
	/**
	 * Adds the missing vertices from the edge set <code>edges</code> to the set <code>vertices</code>.
	 * This assumes that the set of edges is valid and each vertex of the edges
	 * has to be defined in the vertex set.
	 * @param vertices Set of vertices.
	 * @param edges Set of edges.
	 * @param ontName Abbreviated name of the ontology.
	 * @return Expanded and thus valid set of vertices.
	 */
	public static Set<Vertex<Long, VertexValue>> addMissingVertices (Set<Vertex<Long, VertexValue>> vertices, Set<Edge<Long, Float>> edges, String ontName) {
		Set<Vertex<Long, VertexValue>> validVertices = new HashSet<Vertex<Long, VertexValue>>();
		validVertices.addAll(vertices);
		
		Set<Long> vertexNames = new HashSet<Long>();
		for (Vertex<Long, VertexValue> v : vertices)
			vertexNames.add(v.getId());
	
		for (Edge<Long, Float> e : edges) {
			Long srcName = e.getSource();
			Long trgName = e.getTarget();
			if (! vertexNames.contains(srcName)) {
				Vertex<Long, VertexValue> v = new Vertex<Long, VertexValue>(srcName, new VertexValue(ontName,0));
				v.f1.setConComponent(OntologyParserJSON.component_id++);
				validVertices.add(v);
			}
			if (! vertexNames.contains(trgName)) {
				Vertex<Long, VertexValue> v = new Vertex<Long, VertexValue>(trgName, new VertexValue(ontName,0));
				v.f1.setConComponent(OntologyParserJSON.component_id++);
				validVertices.add(v);
			}
		}		
		return validVertices;
	}
	
	
	/**
	 * Removes all invalid edges from the edge set <code>edges</code>.
	 * An edge is invalid iff at least one of its vertices is not part of <code>vertices</code>.
	 * @param set Set of vertices.
	 * @param edges Set of edges.
	 * @param ontName Name of the ontology.
	 * @return Set of valid edges.
	 */
	public static Set<Edge<Long, Float>> removeInvalidEdges (Set<Vertex<Long, VertexValue>> set, 
			Set<Edge<Long, Float>> edges, String ontName) {
		Set<Edge<Long, Float>> validEdges = new HashSet<Edge<Long, Float>>();
		// a vertex' name is its identifier, thus the set of vertex names is calculated
		Set<Long> vertexNames = new HashSet<Long>();
		for (Vertex<Long, VertexValue> v : set)
			vertexNames.add(v.getId());
		
		OutputToFile out = null;
		if (HolomaConstants.IS_PRINTING_INVALID_EDG)
			out = new OutputToFile (500, "./src/main/resources/invalidEdges_"+ontName+".csv");
		
		for (Edge<Long, Float> e : edges) {
			Long srcName = e.getSource();
			Long trgName = e.getTarget();
			if (vertexNames.contains(srcName) && vertexNames.contains(trgName))
				validEdges.add(e);
			else {
				if (HolomaConstants.IS_PRINTING_INVALID_EDG)
					out.addToBuff(srcName+"\t"+trgName+"\t"+e.getValue());	
			}
		}		
		if (HolomaConstants.IS_PRINTING_INVALID_EDG) {
			out.close();
			System.out.println("Printing invalid edge file to ./src/main/resources/invalidEdges_"+ontName+".csv");
		}		
		return validEdges;
	}
	
	

}
