package holoma.parsing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

import tools.io.OutputToFile;

/**
 * Manages the parsing of the different JSON files.
 * @author max
 *
 */
public class ParsingPoint {

	/** The path of the ontology (and the mapping) files. */
	private final String PATH;
	private final String MAP_FILE = "mapping_modTEST.csv";
	/** The ontology JSON file names. */
	private final String[] ONT_FILES;
	
	private List<Edge<String, Integer>> edges = new ArrayList<Edge<String, Integer>>();
	private List<Vertex<String, String>> vertices = new ArrayList<Vertex<String, String>>();

	
	/**
	 * Constructor.
	 * @param path Path of the ontology files.
	 * @param ontFiles Names of the ontology files.
	 */
	public ParsingPoint (String path, String[] ontFiles) {
		this.PATH = path;
		this.ONT_FILES = ontFiles;
	}	
	
	
	/**
	 * Gets the edges within the specified ontology files.
	 * @return Edges of all ontologies.
	 */
	public List<Edge<String, Integer>> getEdges () {
		if (this.edges.isEmpty())
			parseEdgesVertices();
		return this.edges;
	}
	
	/**
	 * Gets the vertices within the specified ontology files.
	 * @return Vertices of all ontologies.
	 */
	public List<Vertex<String, String>> getVertices () {
		if (this.vertices.isEmpty())
			parseEdgesVertices();
		return this.vertices;
	}
	
	
	/**
	 * Creates the edge file to the specified location.
	 * @param location Location of the edge file.
	 */
	public void printEdgeVertexToFile (String locationEdge, String locationVertex) {
		// create a new edge file
		File fileEdge = new File (locationEdge);
		if (fileEdge.exists()) fileEdge.delete();
		// create a new vertex file
		File fileVertex = new File (locationVertex);
		if (fileVertex.exists()) fileVertex.delete();
		
		if (this.edges.isEmpty() || this.vertices.isEmpty())
			parseEdgesVertices();
		
		// print edges
		OutputToFile out = new OutputToFile (800, locationEdge);
		for (Edge<String, Integer> edge : this.edges) {
			String line = edge.f0+"\t"+edge.f1+"\t"+edge.f2;
			out.addToBuff(line);
		}
		out.close();
		// print vertices
		out = new OutputToFile (800, locationVertex);
		for (Vertex<String, String> vert : this.vertices) {
			String line = vert.f0+"\t"+vert.f1;
			out.addToBuff(line);
		}
		out.close();	
	}
	
	/** Parses all ontology files and
	 *  adds the vertices and edges to the list of vertices and edges of all ontologies, respectively.
	 */
	private void parseEdgesVertices () {
		for (String ontology : this.ONT_FILES) {
			// parse each ontology
			System.out.println("Parsing "+this.PATH+ontology+" ... ");
			File fileOntology = new File (this.PATH+ontology);
			OntologyParserJSON parser = new OntologyParserJSON(getOntologyName(ontology), fileOntology);
			parser.doParsing();
			// add the edges and vertices of the current ontology to 'overall' collections
			this.edges.addAll(parser.getEdgeList());
			this.vertices.addAll(parser.getVertexList());
		}
		// add mapping correspondences to edges
		System.out.println("\nReading "+this.PATH+this.MAP_FILE+" ... ");
		BufferedReader reader = null;
		try {
			reader = new BufferedReader ( new FileReader(this.PATH+this.MAP_FILE));
			String line;
			while ( (line = reader.readLine()) != null ) {
				String[] node = line.split("\t");
				Edge<String, Integer> edge = new Edge<String, Integer>(node[0],node[1],0);
				this.edges.add(edge);
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}	
	
	
	
	/**
	 * Calculates the (abbreviated) name of the ontology by means of the file name.
	 * @param fileName The name of the ontology file.
	 * @return The name of the ontology.
	 */
	private String getOntologyName (String fileName) {
		String ontologyName = fileName.substring(0, fileName.indexOf('.')).toLowerCase();
		if (ontologyName.equals("full-galen"))
			ontologyName = "galen";
		else if (ontologyName.equals("ncitncbo"))
			ontologyName = "ncit";
		else if (ontologyName.equals("NPOntology01"))
			ontologyName = "natpro";
		else if (ontologyName.equals("radlex_3"))
			ontologyName = "radlex";
		return ontologyName;		
	}
	
	
	

}
