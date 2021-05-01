package wdc.productcorpus.datacreator.OutputFilesCreator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import wdc.productcorpus.datacreator.Profiler.SpecTables.SpecificationTable;
import wdc.productcorpus.util.InputUtil;

public class TrainingPairs {
	
	public static void main (String args[]) throws IOException {
		
		File inputDir = new File(args[0]);
		File outputDir = new File (args[1]);
		File offers_file = new File(args[2]);
		
		TrainingPairs trainingtranslate = new TrainingPairs();
		HashMap<Integer, JsonObject> offers = trainingtranslate.loadNonNormOffers(offers_file);
		System.out.println("Loaded offers");
		
		for (File f : inputDir.listFiles()) {
			System.out.println("Translating file:"+f.getName());
			if (!f.isDirectory()) {
				HashMap<String, JsonObject> trainingpairs = trainingtranslate.loadV2Pairs(f);
				trainingtranslate.merge(trainingpairs,offers,new File(outputDir+f.getName().substring(0, f.getName().lastIndexOf('.'))));
			}
		}
	}

	public HashMap<String, JsonObject> loadV2Pairs(File pairsFile) throws IOException{
		
		HashMap<String,JsonObject> pair_objects = new HashMap<String,JsonObject>();
		BufferedReader br = InputUtil.getBufferedReader(pairsFile);
		String line;
		while ((line = br.readLine()) != null) {
			JsonObject json = new JsonParser().parse(line).getAsJsonObject();
			String pair_id = json.get("pair_id").getAsString();
			pair_objects.put(pair_id, json);
		}
		br.close();
		return pair_objects;
	}
	
	public HashMap<Integer, JsonObject> loadNonNormOffers(File offersFile) throws IOException{
		
		HashMap<Integer,JsonObject> offers = new HashMap<Integer,JsonObject>();
		BufferedReader br = InputUtil.getBufferedReader(offersFile);
		String line;
		int lineCounter = 0;
		while ((line = br.readLine()) != null) {
			lineCounter++;
			if (lineCounter%1000000==0) System.out.println("Loaded "+lineCounter+" offers");
			JsonObject json = new JsonParser().parse(line).getAsJsonObject();
			Integer node_id = json.get("id").getAsInt();
			offers.put(node_id, json);
		}
		br.close();
		return offers;
	}
	
	public void merge(HashMap<String,JsonObject> pairs, HashMap<Integer,JsonObject> offers, File outputFile) throws IOException{
		
		ArrayList<JsonObject> updatedPairs = new ArrayList<JsonObject>();
		for (Map.Entry<String, JsonObject> pair:pairs.entrySet()){
			
			Integer node_left_id = pair.getValue().get("id_left").getAsInt();
			Integer node_right_id = pair.getValue().get("id_right").getAsInt();
			try {
				pair.getValue().remove("brand_left");
				pair.getValue().remove("brand_right");
				pair.getValue().remove("description_left");
				pair.getValue().remove("description_right");
				pair.getValue().remove("keyValuePairs_left");
				pair.getValue().remove("keyValuePairs_right");
				pair.getValue().remove("price_left");
				pair.getValue().remove("price_right");
				pair.getValue().remove("specTableContent_left");
				pair.getValue().remove("specTableContent_right");
				pair.getValue().remove("title_left");
				pair.getValue().remove("title_right");
				
				JsonObject offer_right = offers.get(node_right_id);
				if (offer_right.isJsonNull()) System.out.println("Could not locate offer with id:"+node_right_id);
				
				JsonObject offer_left = offers.get(node_left_id);
				if (offer_left.isJsonNull()) System.out.println("Could not locate offer with id:"+node_left_id);

				
				pair.getValue().add("brand_left", offer_left.get("brand"));
				pair.getValue().add("brand_right", offer_right.get("brand"));
				
				pair.getValue().add("description_left", offer_left.get("description"));
				pair.getValue().add("description_right", offer_right.get("description"));
				
				pair.getValue().add("keyValuePairs_left", offer_left.get("keyValuePairs"));
				pair.getValue().add("keyValuePairs_right", offer_right.get("keyValuePairs"));
				
				pair.getValue().add("price_left", offer_left.get("price"));
				pair.getValue().add("price_right", offer_right.get("price"));
				
				pair.getValue().add("specTableContent_left", offer_left.get("specTableContent"));
				pair.getValue().add("specTableContent_right",offer_right.get("specTableContent"));
				
				pair.getValue().add("title_left", offer_left.get("title"));
				pair.getValue().add("title_right", offer_right.get("title"));
				
				updatedPairs.add(pair.getValue());
			}
			catch(Exception e){
				System.out.println(e.getMessage());
				System.out.println("An error occured for nodes"+node_left_id+"----"+node_right_id);
			}
		}
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputFile));
		
		for (JsonObject p: updatedPairs) {
			writer.write(p+"\n");
		}
		writer.flush();
		writer.close();
	}
	
}
