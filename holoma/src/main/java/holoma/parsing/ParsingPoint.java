package holoma.parsing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

import holoma.HolomaConstants;
import tools.io.OutputToFile;

/**
 * Manages the parsing of the different JSON files.
 * @author max
 *
 */
public class ParsingPoint {

	/** Edges of the graph. */
	private Set<Edge<String, Integer>> edges = new HashSet<Edge<String, Integer>>();
	/** Vertices of the graph. */
	private Set<Vertex<String, String>> vertices = new HashSet<Vertex<String, String>>();
	

	/** Constructor. */
	public ParsingPoint () {
		
	}	
	
	
	/**
	 * Gets the edges within the specified ontology files.
	 * @return Edges of all ontologies.
	 */
	public Set<Edge<String, Integer>> getEdges () {
		if (this.edges.isEmpty())
			parseEdgesVertices();
		return this.edges;
	}
	
	
	/**
	 * Gets the vertices within the specified ontology files.
	 * @return Vertices of all ontologies.
	 */
	public Set<Vertex<String, String>> getVertices () {
		if (this.vertices.isEmpty())
			parseEdgesVertices();
		return this.vertices;
	}
	
	
	/**  Creates the edge file to the specified location. */
	public void printEdgeVertexToFile () {
		// create a new edge file
		File fileEdge = new File (HolomaConstants.EDGE_FILE_LOC);
		if (fileEdge.exists()) fileEdge.delete();
		// create a new vertex file
		File fileVertex = new File (HolomaConstants.VERTEX_FILE_LOC);
		if (fileVertex.exists()) fileVertex.delete();
		
		if (this.edges.isEmpty() || this.vertices.isEmpty())
			parseEdgesVertices();
		
		// print edges
		OutputToFile out = new OutputToFile (800, HolomaConstants.EDGE_FILE_LOC);
		for (Edge<String, Integer> edge : this.edges) {
			String line = edge.f0+"\t"+edge.f1+"\t"+edge.f2;
			out.addToBuff(line);
		}
		out.close();
		// print vertices
		out = new OutputToFile (800, HolomaConstants.VERTEX_FILE_LOC);
		for (Vertex<String, String> vert : this.vertices) {
			String line = vert.f0+"\t"+vert.f1;
			out.addToBuff(line);
		}
		out.close();	
	}
	
	
	/** Resets the sets of edges and vertices. */
	public void clear() {
		this.edges.clear();
		this.vertices.clear();
	}
	
	
	/** Parses all ontology files and
	 *  adds the vertices and edges to the set of vertices and edges of all ontologies, respectively. 
	 *  @exception Wrong input file. 
	 */
	private void parseEdgesVertices () throws IllegalArgumentException {
		// #1: parse each ontology
		for (String ontology : HolomaConstants.ONTOLOGY_FILES) {
			System.out.println("Parsing "+HolomaConstants.PATH+ontology+" ... ");
			File fileOntology = new File (HolomaConstants.PATH+ontology);
			String ontName = getOntologyName(ontology);
			
			OntologyParserJSON parser = new OntologyParserJSON(ontName, fileOntology);
			parser.doParsing();
			// preprocessing and 
			// add the edges and vertices of the current ontology to 'overall' collections
			if (HolomaConstants.IS_OPTIM_PREPR)
				doOptimPreprocessing(parser.getVertexSet(), parser.getEdgeSet(), ontName);
			else
				doPessimPreprocessing(parser.getVertexSet(), parser.getEdgeSet(), ontName);			
		}
		
		// #2: add mapping correspondences to edges
		//		... and missing vertices to the vertex set
		System.out.println("\nReading "+HolomaConstants.PATH+HolomaConstants.MAPPING_FILE+" ... ");
		BufferedReader reader = null;
		try {
			reader = new BufferedReader ( new FileReader(HolomaConstants.PATH+HolomaConstants.MAPPING_FILE));
			String line;
			while ( (line = reader.readLine()) != null ) {
				String[] fields = line.split(",");
				// check whether it is the right mapping file
				if (fields.length != 6) {
					reader.close();
					throw new IllegalArgumentException("Mapping file has "+fields.length+" columns. 6 expected!");
				}
				Edge<String, Integer> edge = new Edge<String, Integer>(fields[0],fields[1],0);
				this.edges.add(edge);
				edge = new Edge<String, Integer>(fields[1],fields[0],0);
				this.edges.add(edge);
				Vertex<String, String> v1 = new Vertex<String, String>(fields[0],fields[2].toLowerCase());
				Vertex<String, String> v2 = new Vertex<String, String>(fields[1],fields[3].toLowerCase());
				this.vertices.add(v1);
				this.vertices.add(v2);
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Does optimistic preprocessing.
	 * @param vertices Set of vertices.
	 * @param edges Set of edges.
	 * @param ontName Abbreviated name of the ontology.
	 */
	private void doOptimPreprocessing (Set<Vertex<String, String>> vertices, Set<Edge<String, Integer>> edges, String ontName) {
		this.edges.addAll(edges);
		this.vertices.addAll(Preprocessor.addMissingVertices(vertices, edges, ontName));
	}
	
	
	/**
	 * Does pessimistic preprocessing.
	 * @param vertices Set of vertices.
	 * @param edges Set of edges.
	 * @param ontName Abbreviated name of the ontology.
	 */
	private void doPessimPreprocessing (Set<Vertex<String, String>> vertices, Set<Edge<String, Integer>> edges, String ontName) {
		this.edges.addAll(Preprocessor.removeInvalidEdges(vertices, edges, ontName));
		this.vertices.addAll(vertices);
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
		else if (ontologyName.equals("npontology01"))
			ontologyName = "natpro";
		else if (ontologyName.equals("radlex_3"))
			ontologyName = "radlex";
		return ontologyName;		
	}
	
	
	

}
