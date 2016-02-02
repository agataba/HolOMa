package holoma.complexDatatypes;

import java.io.Serializable;

public class EdgeValue implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public int type;
	public float weight;
	
	public EdgeValue (int type, float weight) {
		this.type=type;
		this.weight=weight;
	}
	
	@Override
	public String toString () {
		return "["+type+", "+weight+"]";
	}

}
