package wdc.productcorpus.datacreator.Profiler.SpecTables;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import org.json.JSONObject;

import de.dwslab.dwslib.util.io.InputUtil;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;

public class SpecTablesOfferProfiler {

	static File offersFile =new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\offersWParentDescClean.json_english.json");
	static File specificationTablesFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\SpecificationTables");
	static File outputFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\SpecTablesProfiler\\english_keyValuePairs.txt");
	static String categoryOffers ="C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\OffersPerTopCategory\\offers_Office_Products";
	
	public static void main (String args[]) throws IOException {
		
		SpecTablesOfferProfiler profile = new SpecTablesOfferProfiler();
		if (args.length>0) {
			offersFile= new File(args[0]);
			specificationTablesFile = new File(args[1]);
			outputFile = new File(args[2]);
		}
		profile.profileKeyValuePairs();
	}
	
	public void profileKeyValuePairs() throws IOException {
		
		HashMap<Integer, Integer> valuesCount = new HashMap<Integer,Integer>();
		
		//import category specific offers
		HashSet<String> offersKeys = loadOfferKeys(categoryOffers);
		
		System.out.println("[SpecTablesOfferProfiler] Filtered "+offersKeys.size()+" offers for this category.");
		
		//import the offer data
		DataImporter load = new DataImporter(offersFile);
		//load.importOffers();
		ArrayList<OutputOffer> offers = new ArrayList<OutputOffer>(load.importOffersWithFilter(offersKeys));
		
		//add table information
		SpecTablesImporter loadTables = new SpecTablesImporter();
		offers = loadTables.addTableInfoToOffers(offers, specificationTablesFile);
	
		//profile the property you wish
		for (OutputOffer o:offers) {

			Integer currentvaluecount = valuesCount.get(o.getSpecTable().getKeyValuePairsCounter());
			if (null==currentvaluecount) currentvaluecount=0;
			currentvaluecount++;
			valuesCount.put(o.getSpecTable().getKeyValuePairsCounter(), currentvaluecount);
	

		}
		
		// write unique values count
		System.out.println("Write stats about values count");
		BufferedWriter values_writer = new BufferedWriter (new FileWriter(outputFile,false));
		LinkedHashMap<Integer, Integer> sorted_valuesCount = new LinkedHashMap<>(valuesCount);

	
		for (Map.Entry<Integer, Integer> value : sorted_valuesCount.entrySet())
			values_writer.write(value.getKey()+"\t"+value.getValue()+"\n");
		
		values_writer.flush();
		values_writer.close();
	}

	private HashSet<String> loadOfferKeys(String categoryOffers) throws IOException {
		HashSet<String> keys = new HashSet<String>();
		
		BufferedReader reader = InputUtil.getBufferedReader(new File(categoryOffers));
		
		String line="";
	
		while (reader.ready()) {
			line = reader.readLine();
			JSONObject json = new JSONObject(line);
			
			String url= json.get("url").toString();
			String nodeID = json.get("nodeID").toString();
			
			String key = url+nodeID;
			keys.add(key);
		}
			
		return keys;
	}
}
