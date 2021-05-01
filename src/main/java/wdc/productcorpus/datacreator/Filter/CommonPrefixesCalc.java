package wdc.productcorpus.datacreator.Filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.util.SortMap;

public class CommonPrefixesCalc {
	
	File inputFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\offers_clean.txt");
	File outputFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\prefixes");

	public static void main(String[] args) throws JSONException, IOException {
		CommonPrefixesCalc calculate =new CommonPrefixesCalc();
		if(args.length>0) {
			
			calculate.inputFile= new File(args[0]);
			calculate.outputFile = new File (args[1]);
		}
		
		calculate.commonPrefixCalculator();

	}

	
	public void commonPrefixCalculator() throws JSONException, IOException {
		
		BufferedReader reader = new BufferedReader(new FileReader(inputFile));
		String line= "";
		DataImporter importOffer = new DataImporter();
		HashMap<String,Integer> prefixes =new HashMap<String,Integer>();
		int lineCounter = 0;
		
		while ((line=reader.readLine())!=null) {
			
			lineCounter++;
			if (lineCounter%5000000 == 0) System.out.println("Parsed : "+lineCounter);
			JSONObject json = new JSONObject(line);
			
			JSONArray identifiers = (JSONArray) json.get("identifiers");
			HashMap<String, HashSet<String>> identifier_values = importOffer.getValuesFromJSONArray(identifiers, true);
			
			for(HashSet<String> id_values:identifier_values.values()) {
				for(String value:id_values){
					
					if(value.length() <= 5) continue;
					
					String gram_3 = value.substring(0, 3);
					String gram_4 = value.substring(0,4);
					String gram_5 = value.substring(0,5);
					
					Integer ex_oc_3 = prefixes.get(gram_3);
					if(ex_oc_3==null) ex_oc_3=0;
					ex_oc_3++;
					prefixes.put(gram_3, ex_oc_3);
					
					Integer ex_oc_4 = prefixes.get(gram_4);
					if(ex_oc_4==null) ex_oc_4=0;
					ex_oc_4++;
					prefixes.put(gram_4, ex_oc_4);
					
					Integer ex_oc_5 = prefixes.get(gram_5);
					if(ex_oc_5==null) ex_oc_5=0;
					ex_oc_5++;
					prefixes.put(gram_5, ex_oc_5);
					
				}
			}
		}
		
		reader.close();
		
		BufferedWriter values_writer = new BufferedWriter (new FileWriter(outputFile));
		LinkedHashMap<String, Integer> sorted_prefixes = SortMap.sortByValue(prefixes);

		
		for (Map.Entry<String, Integer> value : sorted_prefixes.entrySet())
			values_writer.write(value.getKey()+"\t"+value.getValue()+"\n");
		
		values_writer.flush();
		values_writer.close();
	}
}
