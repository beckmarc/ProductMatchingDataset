package wdc.productcorpus.datacreator.Profiler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;

public class IDPropsOverlap {

	String prop1 = "/gtin13";
	String prop2 = "/sku";
	File offers = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\test");
	
	public static void main (String args[]) throws JSONException, IOException {
		
		IDPropsOverlap calculate = new IDPropsOverlap();
		
		if (args.length>0) {
			calculate.prop1 = args[0];
			calculate.prop2 = args[1];
			calculate.offers = new File(args[2]);
			
		}
		calculate.calculateOverlap();
	}
	
	public void calculateOverlap() throws JSONException, IOException {
		
		HashSet<String> prop1_values = new HashSet<String>();
		HashSet<String> prop2_values = new HashSet<String>();
		
		double containment=0.0;
		
		DataImporter load = new DataImporter();

		BufferedReader reader = new BufferedReader(new FileReader(offers));
		
		String line = "";
		while ((line=reader.readLine())!=null) {
			
			JSONObject json = new JSONObject(line);

			JSONArray identifiers = (JSONArray) json.get("identifiers");
			HashMap<String, HashSet<String>> identifier_values = load.getValuesFromJSONArray(identifiers, true);
			
			if(identifier_values.containsKey(prop1))
				prop1_values.addAll(identifier_values.get(prop1));
			
			if(identifier_values.containsKey(prop2))
				prop2_values.addAll(identifier_values.get(prop2));
			
		}
		
		reader.close();
		
		if (prop1_values.size()>prop2_values.size()) containment = containment(prop1_values,prop2_values);
		else containment = containment(prop2_values,prop1_values);
		
		System.out.println("Size of "+prop1+" : "+prop1_values.size());
		System.out.println("Size of "+prop2+" : "+prop2_values.size());

		System.out.println("Containment of "+prop1+" and "+prop2+" : "+containment);
	}
	
	public double containment(HashSet<String> longerList, HashSet<String> shorterList){
		
		if (shorterList.size()==0) return 0;
				
		HashSet<String> overlap = new HashSet<>(shorterList);
		overlap.retainAll(longerList);
		System.out.println("Overlap#: "+overlap.size());
		return ((double)overlap.size()/(double)shorterList.size());
		
	}
}
