package holoma;

/**
 * Manages all constants which are relevant for HolOMa.
 * @author max
 *
 */
public abstract class HolomaConstants {

	/** Where to print or where to get the set of edges of the graph. */
	public static final String EDGE_FILE_LOC = "./src/main/resources/edges.csv";
	
	/** Where to print or where to get the set of vertices of the graph. */
	public static final String VERTEX_FILE_LOC = "./src/main/resources/vertices.csv";
	
	/** Where to print the connected components. */
	public static final String CONNCOMP_FILE_LOC = "./src/main/resources/connectedComponents.csv";
	
	
	/** Path of the ontology and mapping files. */
	public static final String PATH = "./src/main/resources/ont/";
	
	/** Name of the mapping file (same path as ontology files). */
	public static final String MAPPING_FILE_UNCOLOR = "mapping.csv";
	public static final String MAPPING_FILE = "mapping_color.csv"; // for testing
	
	
	/** Names of the ontology files. */
	public static final String[] ONTOLOGY_FILES_UNCOLOR = {
		"RXNORM.ttljsonLD.json",
		"PDQ.ttljsonLD.json",
		"NPOntology01.owljsonLD.json",
		"full-galen.owljsonLD.json",
		"MESH.ttljsonLD.json",
		"OMIM.ttljsonLD.json",
		"Radlex_3.12.owljsonLD.json",
		"chebi.owljsonLD.json",
		"fma.owljsonLD.json"/*,
		"NCITNCBO.ttljsonLD.json"
*/	};
	
	public static final String[] ONTOLOGY_FILES = { // for testing
		"blue.ttljsonLD.json",
		"green.ttljsonLD.json",
		"orange.ttljsonLD.json"
	};
	
	
	/** Specifies whether the preprocessor is optimistic: 'true' for optimistic, 'false' for pessimistic.
	 * An optimistic preprocessor adds missing edges' vertices to the set of all vertices.
	 * A pessimistic preprocessor deletes all edges where at least on vertex is not part of the set of all vertices. 
	 * Blank nodes are always deleted. */
	public static final boolean IS_OPTIM_PREPR = true;
	
	/** Invalid edges are printed iff 'true'.
	 *  Invalid edges are calculated iff <code>isOptimPrepr=false</code>. */
	public static final boolean IS_PRINTING_INVALID_EDG = false;
	
	/** Valid edges and vertices are printed iff 'true'. */
	public static final boolean IS_PRINTING_VALID_EDGVERT = false;
	
	/** Maximum number of iteration steps for connected components. */
	public static final int MAX_ITER = 10;
	
	/** Singletons of connected components are eliminated iff 'true'. */
	public static final boolean NO_SINGLETON_CONNCOMP = true;
	
}
