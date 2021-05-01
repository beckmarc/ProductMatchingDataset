package wdc.productcorpus.datacreator.OutputFilesCreator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import wdc.productcorpus.datacreator.Profiler.SpecTables.SpecificationTable;
import wdc.productcorpus.util.InputUtil;

public class OutputOfferV2 extends OutputOffer{
	
	public static void main(String []args) throws IOException{
		OutputOfferV2 translate = new OutputOfferV2();
		
		File mappingFile = new File(args[0]);
		File categoryFile = new File(args[1]);
		File specTableFile = new File(args[2]);
		File offersFile = new File(args[3]);
		File outputFile = new File(args[4]);
		
		HashMap<String,Integer> keymappings = translate.loadMappings(mappingFile);
		HashMap<Integer,String> categorymappings = translate.loadCategoryInfo(categoryFile);
		HashMap<String, SpecificationTable> specTableMappings = translate.loadSpecTables(specTableFile);
		
		translate.translateOffers(offersFile,keymappings, specTableMappings,categorymappings, outputFile);
		
	}

	//load mappings
	public HashMap<String,Integer> loadMappings(File mappingFile) throws IOException {
		HashMap<String,Integer> mappings = new HashMap<String,Integer>();
		BufferedReader br = InputUtil.getBufferedReader(mappingFile);
		String line;
		int counter =0;
		while ((line = br.readLine()) != null) {
			counter++;

			if (counter==1) continue;
			Pattern p = Pattern.compile("(.*),(.*)");
			Matcher m = p.matcher(line);
			if (m.find()) {
				String old_id = m.group(1); //nodeid url
				if (old_id.startsWith("\"")) old_id = old_id.substring(1);
				if (old_id.endsWith("\"")) old_id = old_id.substring(0, old_id.length() - 1);
				Integer new_id = Integer.valueOf(m.group(2));
				mappings.put(old_id, new_id);

			}
			
		}
		br.close();
		System.out.println("Loaded mapped keys: "+mappings.size());
		return mappings;
	}
	
	//load category info
	public HashMap<Integer,String> loadCategoryInfo(File categoryFile) throws IOException {
		HashMap<Integer,String> mappings = new HashMap<Integer,String>();
		BufferedReader br = InputUtil.getBufferedReader(categoryFile);
		String line;
		int counter = 0;
		while ((line = br.readLine()) != null) {
			counter ++;
			if (counter ==1) continue;
			Integer clusterid = Integer.valueOf(line.split(",")[1]);;
			String category = line.split(",")[0];
			mappings.put(clusterid, category);
		}
		br.close();
		System.out.println("Loaded category info for clusters:"+mappings.size());

		return mappings;
	}
	
	public HashMap<String, SpecificationTable> loadSpecTables(File specTableFile) throws IOException{
		HashMap<String,SpecificationTable> mappings = new HashMap<String,SpecificationTable>();
		BufferedReader br = InputUtil.getBufferedReader(specTableFile);
		String line;
		while ((line = br.readLine()) != null) {
			JsonObject json = new JsonParser().parse(line).getAsJsonObject();
			String url = json.get("url").getAsString();
			String content = json.get("specTableContent").getAsString();
			JsonObject keyvaluepairs = json.get("keyValuePairs").getAsJsonObject();
			SpecificationTable table = new SpecificationTable();
			table.setContent(content);
			table.setKeyValuePairs(keyvaluepairs);
			mappings.put(url, table);
		}
		br.close();
		System.out.println("Loaded specification tables");

		return mappings;
	}
	
	public void translateOffers(File offersFile, HashMap<String,Integer> keymap, HashMap<String, SpecificationTable> tablemap, HashMap<Integer,String> categorymap, File outputFile) throws IOException {
		ArrayList<JsonObject> newOffers = new ArrayList<JsonObject>();
		BufferedReader br = InputUtil.getBufferedReader(offersFile);
		String line;
		OutputOffer o = new OutputOffer();
		int newOffersCounter = 0;
		while ((line = br.readLine()) != null) {
			JSONObject json = new JSONObject(line);
			OutputOffer offer = o.jsonToOffer(json);
			
			JsonObject translated_offer = new JsonObject();
			
			//get all info you need for that offer
			Integer mappedid =  keymap.get(offer.getNodeID()+" "+offer.getUrl());
			if (mappedid==null) {
				System.out.println("Could not find any mapping for id:"+offer.getNodeID()+" "+offer.getUrl());
			}
			translated_offer.addProperty("id", mappedid);
			translated_offer.addProperty("cluster_id", Integer.valueOf(offer.getCluster_id()));
			translated_offer.addProperty("category", categorymap.get(Integer.valueOf(offer.getCluster_id())));

			JsonArray id_array = new JsonArray();
			for (Map.Entry<String, HashSet<String>> ids : offer.getIdentifiers().entrySet()){
				JsonObject identifier = new JsonObject();
				identifier.addProperty(ids.getKey(), ids.getValue().toString());
				id_array.add(identifier);
			}
			translated_offer.add("identifiers", id_array);
			
			String ownTitle = null != offer.getDescProperties().get("/title") ? String.join(" ", offer.getDescProperties().get("/title")) : "";
			String ownName = null != offer.getDescProperties().get("/name") ? String.join(" ", offer.getDescProperties().get("/name")) : "";
			String parentTitle = null != offer.getParentdescProperties().get("/title") ? String.join(" ", offer.getParentdescProperties().get("/title")) : "";
			String parentName = null != offer.getParentdescProperties().get("/name") ? String.join(" ", offer.getParentdescProperties().get("/name")) : "";
			
			translated_offer.addProperty("title", getMappedTitle(ownTitle, ownName, parentTitle, parentName));
			
		
			String ownDesc = null != offer.getDescProperties().get("/description") ? String.join(" ", offer.getDescProperties().get("/description")) : "";
			String parentDesc = null != offer.getParentdescProperties().get("/description") ? String.join(" ", offer.getParentdescProperties().get("/description")) : "";
			
			translated_offer.addProperty("description", getMappedDescription(ownDesc, parentDesc));
			
			String ownBrand = null != offer.getDescProperties().get("/brand") ? String.join(" ", offer.getDescProperties().get("/brand")) : "";
			String ownManuf = null != offer.getDescProperties().get("/manufacturer") ? String.join(" ", offer.getDescProperties().get("/manufacturer")) : "";
			String parentBrand = null != offer.getParentdescProperties().get("/brand") ? String.join(" ", offer.getParentdescProperties().get("/brand")) : "";
			String parentManuf = null != offer.getParentdescProperties().get("/manufacturer") ? String.join(" ", offer.getParentdescProperties().get("/manufacturer")) : "";
			
			String mappedbrand = "";
			if (ownBrand!="")
				mappedbrand=ownBrand;
			else if (ownManuf!="")
				mappedbrand=ownManuf;
			else if (parentBrand!="")
				mappedbrand=parentBrand;
			else if (parentManuf!="")
				mappedbrand=parentManuf;
			else mappedbrand=null;
				
			translated_offer.addProperty("brand", mappedbrand);

			String ownPrice = null != offer.getDescProperties().get("/price") ? String.join(" ", offer.getDescProperties().get("/price")) : "";
			String ownPriceCur = null != offer.getDescProperties().get("/priceCurrency") ? String.join(" ", offer.getDescProperties().get("/priceCurrency")) : "";
			String parentPrice = null != offer.getParentdescProperties().get("/price") ? String.join(" ", offer.getParentdescProperties().get("/price")) : "";
			String parentPriceCur = null != offer.getParentdescProperties().get("/priceCurrency") ? String.join(" ", offer.getParentdescProperties().get("/priceCurrency")) : "";
		
			translated_offer.addProperty("price", getMappedPrice(ownPrice, ownPriceCur, parentPrice, parentPriceCur));
			
			if (!tablemap.containsKey(offer.getUrl())){
				translated_offer.add("keyValuePairs",null);
				translated_offer.add("specTableContent", null);
			}
			else {
				JsonObject keyvaluepairs = tablemap.get(offer.getUrl()).getKeyValuePairs();
				if (keyvaluepairs.toString().equals("{}")) translated_offer.add("keyValuePairs",null);
				else translated_offer.add("keyValuePairs", tablemap.get(offer.getUrl()).getKeyValuePairs());
				
				if (tablemap.get(offer.getUrl()).getContent().isEmpty()) translated_offer.add("specTableContent", null);
				else translated_offer.addProperty("specTableContent", tablemap.get(offer.getUrl()).getContent());
			}
			
			
			newOffersCounter++;
			newOffers.add(translated_offer);
			
			if (newOffers.size()==50000){
				System.out.println("Translated offers:"+newOffersCounter);
				BufferedWriter writer = new BufferedWriter (new FileWriter(outputFile, true));
				
				for (JsonObject newoffer: newOffers) {
					writer.write(newoffer+"\n");
				}
				writer.flush();
				writer.close();
				//empty the arraylist
				newOffers = new ArrayList<JsonObject>(); 
			}
		}
		br.close();
		//one last time
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputFile, true));
		
		for (JsonObject newoffer: newOffers) {
			writer.write(newoffer+"\n");
		}
		System.out.println("Translated offers:"+newOffersCounter);

		writer.flush();
		writer.close();
	}

	
	private String getMappedPrice(String ownPrice, String ownPriceCur, String parentPrice, String parentPriceCur) {
		
		String mappedPrice =ownPrice;
		if (mappedPrice=="") mappedPrice=parentPrice;
		if (mappedPrice=="") return null;
		if (ownPriceCur != "" && !mappedPrice.contains(ownPriceCur))
			mappedPrice = mappedPrice + " "+ownPriceCur;
		else if (parentPriceCur!="" && !mappedPrice.contains(parentPriceCur))
			mappedPrice = mappedPrice + " "+parentPriceCur;
		
		return mappedPrice;
		
	}

	private String getMappedDescription(String ownDesc, String parentDesc) {
		String mappedAtt = ownDesc;
		ArrayList<String> desc_par_list =  new ArrayList<> (Arrays.asList(parentDesc.split("\\s+")));

			for (String extratoken:desc_par_list){
			if (!( new ArrayList<> (Arrays.asList(mappedAtt.split("\\s+"))).contains(extratoken)))
				mappedAtt = mappedAtt + " "+ extratoken;
		}
		if (mappedAtt=="") return null;
		else return mappedAtt;
	}

	
	private String getMappedTitle(String title_own, String name_own, String title_par, String name_par) {
				
		String mappedAtt = title_own;
		ArrayList<String> own_name_list =  new ArrayList<> (Arrays.asList(name_own.split("\\s+")));
		ArrayList<String> title_par_list = new ArrayList<>  (Arrays.asList(title_par.split("\\s+")));
		ArrayList<String> name_par_list = new ArrayList<>  (Arrays.asList(name_par.split("\\s+")));
		
		ArrayList<String> tokensByPriority = new ArrayList<String>();
		tokensByPriority.addAll(own_name_list);
		tokensByPriority.addAll(title_par_list);
		tokensByPriority.addAll(name_par_list);
		
		for (String extratoken:tokensByPriority){
			if (!( new ArrayList<> (Arrays.asList(mappedAtt.split("\\s+"))).contains(extratoken)))
				mappedAtt = mappedAtt + " "+ extratoken;
		}
		
		return mappedAtt;
	}

	
	
	
	
	
}
