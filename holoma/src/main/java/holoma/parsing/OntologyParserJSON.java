package holoma.parsing;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

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
	/** The list of the edges. */
	private List<Edge<String, Integer>> edgeList = new ArrayList<Edge<String, Integer>>();
	/** The list of the vertices. */
	private List<Vertex<String, String>> vertexList = new ArrayList<Vertex<String, String>>();
	
	
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
	 * Get the list of edges.
	 * @return Edges.
	 */
	public List<Edge<String, Integer>> getEdgeList ( ) { return this.edgeList; }
	
	/**
	 * Get the list of vertices.
	 * @return Vertices.
	 */
	public List<Vertex<String, String>> getVertexList ( ) { return this.vertexList; }
	
	
	
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
					//TODO: resolve context??
					Vertex<String, String> vertex = new Vertex<String, String>(graphID, this.ONT_NAME);
					this.vertexList.add(vertex);
					
					// get subclasses
					String typeSubclassField = graph_dependency_object.get("subClassOf").getClass().getName();
					// only one parent: field is a string
					if (typeSubclassField.equals("java.lang.String")) {
						String subclass = enrichBlankNode(graph_dependency_object.get("subClassOf").toString());
						Edge<String, Integer> edge = new Edge<String, Integer>(graphID, subclass, 1);
						this.edgeList.add(edge);
					}
					// more than on parent: field is an array
					else {
						JSONArray jArray = graph_dependency_object.getJSONArray("subClassOf");
						for (int j=0; j<jArray.length(); j++){
							String subclass = enrichBlankNode(jArray.getString(j));
							Edge<String, Integer> edge = new Edge<String, Integer>(graphID, subclass, 1);
							this.edgeList.add(edge);
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
}
