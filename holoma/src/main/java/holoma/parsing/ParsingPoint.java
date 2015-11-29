package holoma.parsing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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
	/** The mapping file name. */
	private final String MAP_FILE;
	/** The ontology JSON file names. */
	private final String[] ONT_FILES;
	/** Specifies whether the preprocessor is optimistic.
	 * An optimistic preprocessor adds missing edges' vertices to the set of all vertices.
	 * A pessimistic preprocessor deletes all edges where at least on vertex is not part of the set of all vertices. */
	private final boolean IS_OPTIM_PREPR;
	
	/** Edges of the graph. */
	private Set<Edge<String, Integer>> edges = new HashSet<Edge<String, Integer>>();
	/** Vertices of the graph. */
	private Set<Vertex<String, String>> vertices = new HashSet<Vertex<String, String>>();
	

	/**
	 * Constructor.
	 * @param path Path of the ontology files.
	 * @param ontFiles Names of the ontology files.
	 * @param mapFile Name of the mapping file (same path as ontology files).
	 * @param isOptimPrepr Specifies whether the preprocessor is optimistic:
	 * 'true' for optimistic, 'false' for pessimistic. 
	 * An optimistic preprocessor adds missing edges' vertices to the set of all vertices.
	 * A pessimistic preprocessor deletes all edges where at least on vertex is not part of the set of all vertices.
	 */
	public ParsingPoint (String path, String[] ontFiles, String mapFile, boolean isOptimPrepr) {
		this.PATH = path;
		this.MAP_FILE = mapFile;
		this.ONT_FILES = ontFiles;
		this.IS_OPTIM_PREPR = isOptimPrepr;
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
	
	/** Resets the sets of edges and vertices. */
	public void clear() {
		this.edges.clear();
		this.vertices.clear();
	}
	
	
	/** Parses all ontology files and
	 *  adds the vertices and edges to the set of vertices and edges of all ontologies, respectively.
	 */
	private void parseEdgesVertices () {
		// parse each ontology
		for (String ontology : this.ONT_FILES) {
			System.out.println("Parsing "+this.PATH+ontology+" ... ");
			File fileOntology = new File (this.PATH+ontology);
			String ontName = getOntologyName(ontology);
			
			OntologyParserJSON parser = new OntologyParserJSON(ontName, fileOntology);
			parser.doParsing();
			// preprocessing and 
			// add the edges and vertices of the current ontology to 'overall' collections
			if (this.IS_OPTIM_PREPR)
				doOptimPreprocessing(parser.getVertexSet(), parser.getEdgeSet(), ontName);
			else
				doPessimPreprocessing(parser.getVertexSet(), parser.getEdgeSet(), ontName);			
		}
		// add mapping correspondences to edges
		System.out.println("\nReading "+this.PATH+this.MAP_FILE+" ... ");
		BufferedReader reader = null;
		Set<Edge<String, Integer>> correspondences = new HashSet<Edge<String, Integer>>();
		try {
			reader = new BufferedReader ( new FileReader(this.PATH+this.MAP_FILE));
			String line;
			while ( (line = reader.readLine()) != null ) {
				String[] node = line.split("\t");
				Edge<String, Integer> edge = new Edge<String, Integer>(node[0],node[1],0);
				correspondences.add(edge);
			}
			reader.close();
			// determine valid correspondences
			if (this.IS_OPTIM_PREPR) {
				System.err.println("Not defined yet!");
				System.exit(1);
			}
			else {
				Set<Edge<String, Integer>> validCorrespondences = Preprocessor.removeInvalidEdges(this.vertices, correspondences, "correspondences");			
				this.edges.addAll(validCorrespondences);
			}				
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
