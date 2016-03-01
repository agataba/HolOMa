package holoma.complexDatatypes;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Vertex2RankMap implements Serializable{

	@Override
	public String toString() {
		return "Vertex2RankMap [ranking=" + ranking + "]";
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = -9000514950957147137L;
	
	private Map<Long,Float> ranking;
	
	public Vertex2RankMap (){
		this.ranking = new HashMap<Long,Float> ();
		
	}
	
	public void addRankForVertex (Long vid, Float rank){
		this.ranking.put(vid, rank);
	}
	
	public Map<Long,Float> getRankings(){
		return this.ranking;
	}
	
	public void setRanking (Map<Long,Float> map){
		this.ranking = map;
	}
	public Map<Long,Float> copy(){
		Map<Long,Float> copy = new HashMap<Long,Float>();
		for (Entry<Long,Float> e: this.ranking.entrySet()){
			copy.put(e.getKey(), e.getValue());
		}
		return copy;
	}
}
