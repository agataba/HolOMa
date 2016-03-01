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
import holoma.complexDatatypes.EdgeValue;
import holoma.complexDatatypes.VertexValue;
import tools.io.OutputToFile;

/**
 * Manages the parsing of the different JSON files.
 * @author max
 *
 */
public class ParsingPoint {

	/** Edges of the graph. */
	private Set<Edge<Long, Float>> edges = new HashSet<Edge<Long, Float>>();
	/** Vertices of the graph. */
	private Set<Vertex<Long, VertexValue>> vertices = new HashSet<Vertex<Long, VertexValue>>();
	

	/** Constructor. */
	public ParsingPoint () {
		
	}	
	
	
	/**
	 * Gets the edges within the specified ontology files.
	 * @return Edges of all ontologies.
	 */
	public Set<Edge<Long, Float>> getEdges (String path) {
		if (this.edges.isEmpty())
			parseEdgesVertices(path);
		return this.edges;
	}
	
	
	/**
	 * Gets the vertices within the specified ontology files.
	 * @return Vertices of all ontologies.
	 */
	public Set<Vertex<Long, VertexValue>> getVertices (String path) {
		if (this.vertices.isEmpty())
			parseEdgesVertices(path);
		return this.vertices;
	}
	
	
	/**  Creates the edge file to the specified location. */
	public void printEdgeVertexToFile (String path) {
		// create a new edge file
		File fileEdge = new File (HolomaConstants.EDGE_FILE_LOC);
		if (fileEdge.exists()) fileEdge.delete();
		// create a new vertex file
		File fileVertex = new File (HolomaConstants.VERTEX_FILE_LOC);
		if (fileVertex.exists()) fileVertex.delete();
		
		if (this.edges.isEmpty() || this.vertices.isEmpty())
			parseEdgesVertices(path);
		
		// print edges
		OutputToFile out = new OutputToFile (800, HolomaConstants.EDGE_FILE_LOC);
		for (Edge<Long, Float> edge : this.edges) {
			String line = edge.f0+"\t"+edge.f1+"\t"+edge.f2;
			out.addToBuff(line);
		}
		out.close();
		// print vertices
		out = new OutputToFile (800, HolomaConstants.VERTEX_FILE_LOC);
		for (Vertex<Long, VertexValue> vert : this.vertices) {
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
	private void parseEdgesVertices (String path) throws IllegalArgumentException {
		// #1: parse each ontology
		for (String ontology : HolomaConstants.ONTOLOGY_FILES_UNCOLOR) {
			System.out.println("Parsing "+HolomaConstants.PATH+ontology+" ... ");
			File fileOntology = new File (path+File.separator+ontology);
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
		System.out.println("\nReading "+path+File.separator+HolomaConstants.MAPPING_FILE+" ... ");
		BufferedReader reader = null;
		try {
			reader = new BufferedReader ( new FileReader(path+File.separator+HolomaConstants.MAPPING_FILE_UNCOLOR));
			String line;
			while ( (line = reader.readLine()) != null ) {
				String[] fields = line.split(",");
				// check whether it is the right mapping file
				if (fields.length > 7) {
					reader.close();
					throw new IllegalArgumentException("Mapping file has "+fields.length+" columns. 7 expected!");
				}
				long src = Dictionary.getInstance().getId(fields[0]);
				long target = Dictionary.getInstance().getId(fields[1]);
				Edge<Long, Float> edge = new Edge<Long, Float>(src,target,1f);
				Edge<Long, Float> revEdge = new Edge<Long, Float>(target,src,1f);
				if (fields.length==7){
					Float sim = Float.parseFloat(fields[6]);
					edge.setValue(sim);
					revEdge.setValue(sim);
				}
				
				this.edges.add(edge);
				this.edges.add(revEdge);
				Vertex<Long, VertexValue> v1 = new Vertex<Long, VertexValue>(src,new VertexValue(fields[2].toLowerCase(), 0));
				v1.f1.setConComponent(OntologyParserJSON.component_id++);
				Vertex<Long, VertexValue> v2 = new Vertex<Long, VertexValue>(target,new VertexValue(fields[3].toLowerCase(), 0));
				v2.f1.setConComponent(OntologyParserJSON.component_id++);
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
	private void doOptimPreprocessing (Set<Vertex<Long, VertexValue>> vertices, Set<Edge<Long, Float>> edges, String ontName) {
		this.edges.addAll(edges);
		this.vertices.addAll(Preprocessor.addMissingVertices(vertices, edges, ontName));
	}
	
	
	
	/**
	 * Does pessimistic preprocessing.
	 * @param set Set of vertices.
	 * @param edges Set of edges.
	 * @param ontName Abbreviated name of the ontology.
	 */
	private void doPessimPreprocessing (Set<Vertex<Long, VertexValue>> set, Set<Edge<Long, Float>> edges, String ontName) {
		this.edges.addAll(Preprocessor.removeInvalidEdges(set, edges, ontName));
		this.vertices.addAll(set);
	}
	
	
	/**
	 * Calculates the (abbreviated) name of the ontology by means of the file name.
	 * @param fileName The name of the ontology file.
	 * @return The name of the ontology.
	 */
	private String getOntologyName (String fileName) {
		String ontologyName = fileName.substring(0, fileName.indexOf('.'));
		if (ontologyName.equals("full-galen"))
			ontologyName = "galen";
		else if (ontologyName.equals("ncitncbo"))
			ontologyName = "ncit";
		else if (ontologyName.equals("npontology01"))
			ontologyName = "natpro";
		else if (ontologyName.equals("radlex_3"))
			ontologyName = "radlex";
		else if (ontologyName.contains("FMA")){
			ontologyName = "FMA";
		}else if (ontologyName.contains("NCI")){
			ontologyName = "NCIT";
		}else if (ontologyName.contains("SNOMED")){
			ontologyName = "SNMD";
		}else if (ontologyName.contains("UBERON")){
			ontologyName ="UBERON";
		}
		return ontologyName;		
	}
	
	
	

}
