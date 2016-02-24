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
	private Map<String,Float> ranking;
	
	public Vertex2RankMap (){
		this.ranking = new HashMap<String,Float> ();
		
	}
	
	public void addRankForVertex (String vid, Float rank){
		this.ranking.put(vid, rank);
	}
	
	public Map<String,Float> getRankings(){
		return this.ranking;
	}
	
	public void setRanking (Map<String,Float> map){
		this.ranking = map;
	}
	public Map<String,Float> copy(){
		Map<String,Float> copy = new HashMap<String,Float>();
		for (Entry<String,Float> e: this.ranking.entrySet()){
			copy.put(e.getKey(), e.getValue());
		}
		return copy;
	}
}
