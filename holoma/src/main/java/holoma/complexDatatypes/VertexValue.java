package holoma.complexDatatypes;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class VertexValue implements Serializable {
	
	
	private static final long serialVersionUID = 1L;
	private long id ;
	public String ontName;
	private Map<Long,Vertex2RankMap> componentToRank; 
	public String getOntName() {
		return ontName;
	}

	private Long conComponent = -1l;

	private Set<Long> conComponentIds;
	
	public void setOntName(String ontName) {
		this.ontName = ontName;
	}


	public Float getPr() {
		return pr;
	}


	public void setPr(Float pr) {
		this.pr = pr;
	}

	public Float pr;
	
	/**
	 * Create a new vertex value.
	 * @param ontName Name of the ontology the vertex is part of.
	 * @param pr PageRank value of this vertex.
	 */
	public VertexValue (String ontName, float pr) {
		this.ontName=ontName;
		this.pr=pr;
		this.conComponentIds = new HashSet<Long>();
		this.setComponentToRank(new HashMap<Long,Vertex2RankMap>());
	}
	
	
	/** Creates a new vertex value with default values. */
	public VertexValue () {
		this.pr =0f;
		this.ontName =null;
		this.conComponentIds = new HashSet<Long>();
		this.setComponentToRank(new HashMap<Long,Vertex2RankMap>());
	}
	
	public void addCompId (long id){
		this.conComponentIds.add(id);
		
	}
	
	public Set<Long> getCompIds (){
		return this.conComponentIds;
	}
	
	
//	public VertexValue(String ontName2, Float pr2, Long conComponent2) {
//		this.pr =pr2;
//		this.ontName =ontName2;
//		this.conComponentIds = new HashSet<Long>();
//		this.addCompId(conComponent2);
//		this.setComponentToRank(new HashMap<Long,Vertex2RankMap>());
//	}
	
//	public VertexValue(String ontName2, Float pr2, Set<Long> conComponent2) {
//		this.pr =pr2;
//		this.ontName =ontName2;
//		this.conComponentIds = conComponent2;
//		this.setComponentToRank(new HashMap<Long,Vertex2RankMap>());
//	}


	
	
	@Override
	public String toString() {
		return "VertexValue [ontName=" + ontName + ", conComponentIds="
				+ conComponentIds + ", pr=" + pr + "]";
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


	public Long getConComponent() {
		return conComponent;
	}


	public void setConComponent(Long conComponent) {
		this.conComponent = conComponent;
	}


	public Map<Long,Vertex2RankMap> getComponentToRank() {
		return componentToRank;
	}


	public void setComponentToRank(Map<Long,Vertex2RankMap> componentToRank) {
		this.componentToRank = componentToRank;
	}

	public VertexValue clone(){
		VertexValue copy = new VertexValue (this.getOntName(),this.getPr());
		copy.setCompIds(this.getCompIds());
		copy.setComponentToRank(getComponentToRank());
		return copy;
	}


	public void setCompIds(Set<Long> compIds) {
		this.conComponentIds = compIds;
		
	}


	public long getId() {
		return id;
	}


	public void setId(long id) {
		this.id = id;
	}
}
