package holoma.complexDatatypes;

public class VertexValue {
	
	public String ontName;
	public float pr;
	
	/**
	 * Create a new vertex value.
	 * @param ontName Name of the ontology the vertex is part of.
	 * @param pr PageRank value of this vertex.
	 */
	public VertexValue (String ontName, float pr) {
		this.ontName=ontName;
		this.pr=pr;
	}
	
	
	/** Creates a new vertex value with default values. */
	public VertexValue () {
		this.ontName="";
		this.pr=0f;
	}
	
	
	@Override
	public String toString() {
		return "["+ontName+", "+pr+"]";
	}
	

}
