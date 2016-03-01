package holoma.parsing;

import holoma.complexDatatypes.EdgeValue;
import holoma.complexDatatypes.VertexValue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
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
	private Set<Edge<Long, Float>> edgeSet = new HashSet<Edge<Long, Float>>();
	/** The set of the vertices. */
	private Set<Vertex<Long, VertexValue>> vertexSet = new HashSet<Vertex<Long, VertexValue>>();
	
	
	
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
	public Set<Edge<Long, Float>> getEdgeSet ( ) { return this.edgeSet; }
	
	/**
	 * Get the set of vertices.
	 * @return Vertices.
	 */
	public Set<Vertex<Long, VertexValue>> getVertexSet ( ) { return this.vertexSet; }
	
	static Long component_id =0l;
	
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
					long id = Dictionary.getInstance().getId(graphID);
					Vertex<Long, VertexValue> vertex = new Vertex<Long, VertexValue>(id,new VertexValue(this.ONT_NAME,0));
					vertex.f1.setConComponent(component_id++);
					this.vertexSet.add(vertex);
					
					// get subclasses
					String typeSubclassField = graph_dependency_object.get("subClassOf").getClass().getName();
					// only one parent: field is a string
					if (typeSubclassField.equals("java.lang.String")) {
						String subclass = enrichShortHandNode(graph_dependency_object.get("subClassOf").toString());
						long subId = Dictionary.getInstance().getId(subclass);
						// blank nodes are ignored
						if (!subclass.startsWith("_:")) {
							Edge<Long, Float> edge = new Edge<Long, Float>(id, subId,0.5f);
							//TODO check if it works
							Edge<Long, Float> revEdge = new Edge<Long, Float>(subId, id, 0.5f);
							edgeSet.add(revEdge);
							this.edgeSet.add(edge);
						}
					}
					// more than on parent: field is an array
					else {
						JSONArray jArray = graph_dependency_object.getJSONArray("subClassOf");
						for (int j=0; j<jArray.length(); j++){
							String subclass = enrichShortHandNode(jArray.getString(j));
							long subId = Dictionary.getInstance().getId(subclass);
							// blank nodes are ignored
							if (!subclass.startsWith("_:")) {
								Edge<Long, Float> edge = new Edge<Long, Float>(id, subId, 0.5f);
		//TODO check if it works
								Edge<Long,Float> revEdge = new Edge<Long,Float>(subId,id,0.5f);
								this.edgeSet.add(edge);
								this.edgeSet.add(revEdge);
							} // end if
						} // end for j
					} // end else
				} // end for i
			} // end if
		} catch (JSONException  | IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	
	/**
	 * Enriches a node with a short hand name to the full URI name.
	 * @param node A node with a potentially short hand name.
	 * @return Node with a full URI name.
	 */
	private String enrichShortHandNode (String node) {
		if (node.startsWith("biotop:"))
			return  "http://purl.org/biotop/biotop.owl#"+node.substring("biotop:".length());
		if (node.startsWith("chebi:") && this.ONT_NAME.equals("chebi"))
			return "http://purl.obolibrary.org/obo/chebi#"+node.substring("chebi:".length());
		if  (node.startsWith("chebi:") && this.ONT_NAME.equals("natpro"))
			return "http://purl.bioontology.org/ontology/CHEBI#"+node.substring("chebi:".length());
		if (node.startsWith("owl:"))
			return "http://www.w3.org/2002/07/owl#"+node.substring("owl:".length());
		if (node.startsWith("rdf:"))
			return "http://www.w3.org/1999/02/22-rdf-syntax-ns#"+node.substring("rdf:".length());
		else
			return node;
	}
}
