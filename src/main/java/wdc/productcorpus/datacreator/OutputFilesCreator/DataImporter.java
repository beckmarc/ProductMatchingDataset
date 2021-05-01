package wdc.productcorpus.datacreator.OutputFilesCreator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import de.dwslab.dwslib.util.io.InputUtil;

/**
 * @author Anna Primpeli
 * Import the data from the JSON file and creates the OutputOfers elements
 */
public class DataImporter {

	private File inputFile;
	
	public DataImporter(File offersFile) {
		this.inputFile = offersFile;
		
	}
	
	
	
	public DataImporter() {
		super();
	}

	public HashSet<OutputOffer> importOffers() {
		HashSet<OutputOffer> offers = new HashSet<OutputOffer>();

		
		try {
			BufferedReader reader = InputUtil.getBufferedReader(inputFile);
			
			String line="";
			int offerCounter = 0;
			while ((line=reader.readLine())!=null) {
				offerCounter++;
				if (offerCounter%1000000==0) System.out.println("Loaded "+offerCounter+" offers");
				JSONObject json = new JSONObject(line);
				
				OutputOffer offer = jsonToOffer(json);
				
				offers.add(offer);
				
			}
		}
		catch(Exception e){
			System.out.println("[DataImporter]"+e.getMessage());
		}
		
		return offers;
	}
	
	
	
	public OutputOffer jsonToOffer(JSONObject json) {
		
		String url = json.getString("url");
		String nodeID = json.getString("nodeID");
		String clusterID = json.get("cluster_id").toString();
		
		JSONArray desc = (JSONArray) json.get("schema.org_properties");
		HashMap<String, HashSet<String>> desc_values = getValuesFromJSONArray(desc, false);
		JSONArray identifiers = (JSONArray) json.get("identifiers");
		HashMap<String, HashSet<String>> identifier_values = getValuesFromJSONArray(identifiers, true);
		
		HashMap<String, HashSet<String>> non_norm_identifier_values = new HashMap<String, HashSet<String>>();
		if (json.has("non_normalized_identifiers")) {
			JSONArray nonormidentifiers = (JSONArray) json.get("non_normalized_identifiers");			
			non_norm_identifier_values = getValuesFromJSONArray(nonormidentifiers, true);
		}
		
		
		JSONArray parentdescProperties = (JSONArray) json.get("parent_schema.org_properties");
		HashMap<String, HashSet<String>> parentdescProperties_values = getValuesFromJSONArray(parentdescProperties, true);
		
		String propertyToParent = json.get("relationToParent").toString();
		String parentNodeID = json.get("parent_NodeID").toString();
		
		OutputOffer offer = new OutputOffer();
		offer.setUrl(url);
		offer.setNodeID(nodeID);
		offer.setCluster_id(clusterID);
		offer.setDescProperties(desc_values);
		offer.setIdentifiers(identifier_values);
		offer.setParentdescProperties(parentdescProperties_values);
		offer.setParentNodeID(parentNodeID);
		offer.setPropertyToParent(propertyToParent);
		offer.setNonprocessedIdentifiers(non_norm_identifier_values);
		
		return offer;
	} 
	
	
	public OutputOffer jsonToSimpleOffer(JSONObject json) {
		
		String url = json.getString("url");
		String nodeID = json.getString("nodeID");
		String clusterID = json.get("cluster_id").toString();
		
		
		
		OutputOffer offer = new OutputOffer();
		offer.setUrl(url);
		offer.setNodeID(nodeID);
		offer.setCluster_id(clusterID);
		
		
		return offer;
	} 
	
	
	/**
	 * @param array
	 * @param isIdentifiers
	 * @return
	 */
	public HashMap<String, HashSet<String>> getValuesFromJSONArray(JSONArray array, boolean isIdentifiers) {
		
		HashMap<String, HashSet<String>> valuesOfArray = new HashMap();
		
		
		for (int i= 0; i< array.length(); i++) {
			JSONObject o = array.getJSONObject(i);
			Set<String> keys = o.keySet();
			for (String k:keys) {
				HashSet<String> arrayValuesAsSet = new HashSet<String>();
				if (isIdentifiers){
					String [] arrayValues = o.get(k).toString().replaceAll("\\[", "").replaceAll("\\]", "").split(",");				
					for (int j =0; j<arrayValues.length;j++)
						arrayValuesAsSet.add(arrayValues[j]);
				}
				else arrayValuesAsSet.add(o.get(k).toString().replaceAll("\\[", "").replaceAll("\\]", ""));
				
				valuesOfArray.put(k, arrayValuesAsSet);
			}
		}
		
		return valuesOfArray;
	}

	public static void main(String args[]) throws IOException {
		DataImporter load = new DataImporter(new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\OfferInfo.json"));
		HashSet<OutputOffer> offers = load.importOffers();
		System.out.println("Loaded "+offers.size()+" offers");
	}



	public HashSet<OutputOffer> importOffersWithFilter(HashSet<String> offersKeys) {
		HashSet<OutputOffer> offers = new HashSet<OutputOffer>();

		try {
			
			BufferedReader reader = InputUtil.getBufferedReader(inputFile);
			
			String line="";

			while (reader.ready()) {
				line = reader.readLine();
				JSONObject json = new JSONObject(line);
				
				String url = json.get("url").toString();
				String nodeID = json.get("nodeID").toString();
				
				String key = url+nodeID;
				
				if (offersKeys.contains(key)) {
					OutputOffer offer = jsonToOffer(json);
					
					offers.add(offer);
				}
				
			
				
			}
		}
		catch(Exception e){
			System.out.println("[DataImporter]"+e.getMessage());
		}
		return offers;
	}



	public HashMap<String,HashSet<OutputOffer>> importSimpleOfferWClusterFilter(HashSet<OutputCluster> clusters) {
		HashMap<String,HashSet<OutputOffer>> offersPerCluster = new HashMap<String,HashSet<OutputOffer>>();

		
		try {
			BufferedReader reader = InputUtil.getBufferedReader(inputFile);
			
			String line="";
			int offerCounter = 0;
			while ((line=reader.readLine())!=null) {
				offerCounter++;
				if (offerCounter%1000000==0) System.out.println("Loaded "+offerCounter+" offers");
				JSONObject json = new JSONObject(line);
				
				OutputOffer offer = jsonToSimpleOffer(json);
				
				if (clusters.contains(new OutputCluster(offer.getCluster_id()))){
					HashSet<OutputOffer> existingOffers = offersPerCluster.get(offer.getCluster_id());
					if (null == existingOffers) {
						existingOffers = new HashSet<OutputOffer>();
						
					}
					
					existingOffers.add(offer);
					offersPerCluster.put(offer.getCluster_id(), existingOffers);
					
				}
					
				
			}
		}
		catch(Exception e){
			System.out.println("[DataImporter]"+e.getMessage());
		}
		
		return offersPerCluster;
	}



	
}
