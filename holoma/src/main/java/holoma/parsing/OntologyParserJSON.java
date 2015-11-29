package holoma.parsing;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Parser of a JSON ontology file.
 * @author max
 *
 */
public class OntologyParserJSON {
	
	/** The JSON file which contains the ontology. */
	private final File ONT_FILE;
	/** The name of the current ontology which is being parsed. */
	private final String ONT_NAME;
	/** The set of the edges. */
	private Set<Edge<String, Integer>> edgeSet = new HashSet<Edge<String, Integer>>();
	/** The set of the vertices. */
	private Set<Vertex<String, String>> vertexSet = new HashSet<Vertex<String, String>>();
	
	
	
	/**
	 * Constructor.
	 * @param ontName Name of the ontology that has to be parsed.
	 * @param the_json The JSON file which contains the ontology.
	 */
	public OntologyParserJSON (String ontName, File the_json) {
		this.ONT_FILE = the_json;
		this.ONT_NAME = ontName;
	}
	
	/**
	 * Get the set of edges.
	 * @return Edges.
	 */
	public Set<Edge<String, Integer>> getEdgeSet ( ) { return this.edgeSet; }
	
	/**
	 * Get the set of vertices.
	 * @return Vertices.
	 */
	public Set<Vertex<String, String>> getVertexSet ( ) { return this.vertexSet; }
	
	
	
	/** Parses a JSON file which contains ontological information. */
	public void doParsing () {
		try {
			if (ONT_FILE.exists()){
				InputStream is = new FileInputStream(ONT_FILE);

				//the inputStream has to be converted to a String
				String jsonTxt = IOUtils.toString(is);

				//the String is converted to a JSON object
				JSONObject json = new JSONObject(jsonTxt);

				//graph structure converted to an array.
				JSONArray jsonArray= json.getJSONArray("@graph");
				int size = jsonArray.length();
				
				for (int i=0; i < size; i++){
					JSONObject graph_dependency_object = jsonArray.getJSONObject(i);
					// get id
					String graphID = graph_dependency_object.get("@id").toString();
					graphID = enrichShortHandNode(graphID);
			
					Vertex<String, String> vertex = new Vertex<String, String>(graphID, this.ONT_NAME);
					this.vertexSet.add(vertex);
					
					// get subclasses
					String typeSubclassField = graph_dependency_object.get("subClassOf").getClass().getName();
					// only one parent: field is a string
					if (typeSubclassField.equals("java.lang.String")) {
						String subclass = enrichBlankNode(graph_dependency_object.get("subClassOf").toString());
						subclass = enrichShortHandNode(subclass);
						Edge<String, Integer> edge = new Edge<String, Integer>(graphID, subclass, 1);
						this.edgeSet.add(edge);
					}
					// more than on parent: field is an array
					else {
						JSONArray jArray = graph_dependency_object.getJSONArray("subClassOf");
						for (int j=0; j<jArray.length(); j++){
							String subclass = enrichBlankNode(jArray.getString(j));
							subclass = enrichShortHandNode(subclass);
							Edge<String, Integer> edge = new Edge<String, Integer>(graphID, subclass, 1);
							this.edgeSet.add(edge);
						}
					}
				}
			}
		} catch (JSONException  | IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Enriches a blank node identifier with the name of the ontology
	 * in which it occurs.
	 * @param node Locally unique identifier of a node.
	 * @param ontName Name of the ontology.
	 * @return Enriched name of the node.
	 */
	private String enrichBlankNode (String node) {
		return (node.startsWith("_:")) ? this.ONT_NAME+node : node;
	}
	
	
	/**
	 * Enriches a node with a short hand name to the full URI name.
	 * @param node A node with a potentially short hand name.
	 * @return Node with a full URI name.
	 */
	private String enrichShortHandNode (String node) {
		if (node.startsWith("biotop:")) {
			return  "http://purl.org/biotop/biotop.owl#"+node.substring("biotop:".length());
		}
		if (node.startsWith("owl:")) {
			return "http://www.w3.org/2002/07/owl#"+node.substring("owl:".length());
		}
		if (node.startsWith("chebi:") && this.ONT_NAME.equals("natpro")) {
			//TODO: this doesn't work ...
			return "http://purl.bioontology.org/ontology/CHEBI#"+node.substring("chebi:".length());
		}
		else
			return node;
	}
}
