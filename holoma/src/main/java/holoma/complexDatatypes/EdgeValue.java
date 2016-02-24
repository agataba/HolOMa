package holoma.complexDatatypes;

import java.io.Serializable;
import java.util.Map;

public class EdgeValue implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public Integer type;
	public Float weight;
	public String  ont_type;
	private Float overall_inverse_weight;
	
	private Map<Long,Float> component2OverallInverse;
	
	public Map<Long, Float> getComponent2OverallInverse() {
		return component2OverallInverse;
	}

	public void setComponent2OverallInverse(
			Map<Long, Float> component2OverallInverse) {
		this.component2OverallInverse = component2OverallInverse;
	}

	public Map<Long, Float> getComponent2OverallIsA() {
		return component2OverallIsA;
	}

	public void setComponent2OverallIsA(Map<Long, Float> component2OverallIsA) {
		this.component2OverallIsA = component2OverallIsA;
	}

	private Map<Long,Float> component2OverallIsA;
	private Float overall_isA_weight;
	
	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public float getWeight() {
		return weight;
	}

	public void setWeight(float weight) {
		this.weight = weight;
	}

	public String getOnt_type() {
		return ont_type;
	}

	public void setOnt_type(String ont_type) {
		this.ont_type = ont_type;
	}

	
	
	public EdgeValue (){
		
	}
	
	public EdgeValue (int type, float weight) {
		this.type=type;
		this.weight=weight;
	}
	
	public EdgeValue (int type, float weight, String ontologyTarget) {
		this.type=type;
		this.weight=weight;
		this.ont_type = ontologyTarget;
	}
	
	

	@Override
	public String toString() {
		return "EdgeValue [type=" + type + ", weight=" + weight + ", ont_type="
				+ ont_type + ", overall_inverse_weight="
				+ overall_inverse_weight + ", overall_isA_weight="
				+ overall_isA_weight + "]";
	}

	public Float getOverall_inverse_weight() {
		return overall_inverse_weight;
	}

	public void setOverall_inverse_weight(float overall_inverse_weight) {
		this.overall_inverse_weight = overall_inverse_weight;
	}

	public float getOverall_isA_weight() {
		return overall_isA_weight;
	}

	public void setOverall_isA_weight(float overall_isA_weight) {
		this.overall_isA_weight = overall_isA_weight;
	}

}
