package holoma.parsing;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.apache.commons.io.IOUtils;


/**
 * Manages the parsing of the different JSON files.
 * @author max
 *
 */
public class ParsingPoint {


	public static void parseProfilesJson(File the_json) {
		System.out.println(the_json);
		try {
			File f = the_json;
			if (f.exists()){
				InputStream is = new FileInputStream(the_json);

				//the inputStream has to be converted to a String
				String jsonTxt = IOUtils.toString(is);

				//the String is converted to a JSON object
				JSONObject json = new JSONObject(jsonTxt);

				//graph structure converted to an array.
				JSONArray jsonArray= json.getJSONArray("@graph");
				int size = jsonArray.length();
				
				//arrays
				ArrayList<JSONObject> arrays = new ArrayList<JSONObject>();
				for (int i=0; i < size; i++){

					JSONObject graph_dependency_object = jsonArray.getJSONObject(i);

					System.out.println(graph_dependency_object);
					String graphID = graph_dependency_object.get("@id").toString();
					String subClassOf = graph_dependency_object.get("subClassOf").toString();

					/*
					
						JSONArray jArray = graph_dependency_object.getJSONArray("subClassOf");
						int jArraySize = jArray.length();

						for (int j=0; j<jArraySize; j++){
							JSONObject object3 = jArray.getJSONObject(j);
							//String comp_id = object3.getString("companyid");
							System.out.println("object3 " + object3);
						}
					
					*/

					System.out.println("id:  " + graphID + " subclassof: " + subClassOf);
					System.out.println();
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


}
