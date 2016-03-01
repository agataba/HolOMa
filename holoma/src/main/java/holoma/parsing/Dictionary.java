package holoma.parsing;

import java.util.HashMap;
import java.util.Map;

public class Dictionary {

	
	static Dictionary instance;
	static long id;
	Map<String,Long> dict;
	Map <Long,String> rev;
 	private Dictionary (){
		id = 0;
		dict =new HashMap<String,Long>();
		rev = new HashMap <Long,String>();
	}
	
	
	public Long getId(String uri){
		if (dict.get(uri)!=null){
			return dict.get(uri);
		}else{
			rev.put(id,uri);
			dict.put(uri, id);
			id++;
			return dict.get(uri);
		}
	}
	
	public String getReverse(Long id){
		return rev.get(id);
	}

	
	public static Dictionary getInstance(){
		if (instance ==null){
			instance = new Dictionary();
		}
		return instance;
	}
}

