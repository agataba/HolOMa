package holoma.complexDatatypes;

import java.io.Serializable;

public class VertexValue implements Serializable {
	
	
	private static final long serialVersionUID = 1L;
	
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
		return "["+ontName+", "+Math.round((pr*100)/100.0)+"]";
	}
	
	@Override
	public boolean equals( Object o )
	{
	  if ( o == null )
	    return false;

	  if ( o == this )
	    return true;

	  VertexValue that = (VertexValue) o;

	  return    this.ontName.equals(that.ontName)
	         && Math.abs(this.pr - that.pr) < 0.0000000000001f;
	}

}
