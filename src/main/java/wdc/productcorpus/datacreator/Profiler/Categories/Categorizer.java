package wdc.productcorpus.datacreator.Profiler.Categories;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import de.dwslab.dwslib.util.io.InputUtil;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;

public class Categorizer {

	boolean rescale = false;
	
	@Parameter(names = { "-offersFile",
	"-offersFileDir" }, required = false, description = "File containing offers.", converter = FileConverter.class)
	private File offersFile;
	
//	@Parameter(names = { "-offersMetaFile",
//	"-offersFileMetaDir" }, required = false, description = "Output file containing offers with category information.", converter = FileConverter.class)
//	private File offersMetaFile;
	
	@Parameter(names = { "-lexicon",
	"-lexiconFileDir" }, required = false, description = "Folder containing the external category lexica.", converter = FileConverter.class)
	private File lexiconFile;
	
	@Parameter(names = { "-clustermetaFile",
	"-clustermetaFileDir" }, required = false, description = "File containing the cluster meta info.", converter = FileConverter.class)
	private File clustermetaFile;
		
	@Parameter(names = { "-clustermetaUpdated",
	"-clustermetaUpdatedDir" }, required = false, description = "Output cluster meta info with the category information.", converter = FileConverter.class)
	private File clustermetaUpdated;
	
	@Parameter(names = { "-title",
	"-considerTitle" }, required = false, description = "Consider title for the categorization.")
	private boolean title;
	
	@Parameter(names = { "-description",
	"-considerDescription" }, required = false, description = "Consider description for the categorization.")
	private boolean description;
	
	@Parameter(names = { "-specTables",
	"-considerspecTables" }, required = false, description = "Consider specification tables for the categorization.")
	private boolean specTables;
	
	public static void main(String args[]) throws IOException {
		
		Categorizer cat = new Categorizer();
		cat.offersFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\offers_clean.txt_filteredIDLabels.txt");
		cat.lexiconFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\Lexicon");
		cat.clustermetaFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\clusters.json");
		cat.clustermetaUpdated = new File ("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\clusters_cat.json");
	//	cat.offersMetaFile = new File ("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\offers_cat.json");
		cat.title = true;
		cat.description = true;
		
		cat.categorize();
	}
	
	public void categorize() throws IOException {
		//import the offer data
		DataImporter load = new DataImporter(offersFile);
	
		
		System.out.println("Load Offer items");
		ArrayList<OutputOffer> offers = new ArrayList<OutputOffer>(load.importOffers());
		
		//load the lexicons
		System.out.println("Import Lexicons");
		LexiconsImporter importLexicons = new LexiconsImporter(lexiconFile);
		ArrayList<CategoryScoring> categoryScoring = importLexicons.saveCategoryScoringInfo(rescale);
		
		//category per cluster
		HashMap<String, ClusterScoring> clustersCategorization = new HashMap<String, ClusterScoring>();
		
		System.out.println("Categorize Offers");

//		BufferedWriter writer = new BufferedWriter(new FileWriter(offersMetaFile));
		
		//get the dominant category for every offer
		for (int i = 0; i< offers.size();i++) {
			if (i%1000==0) System.out.println("Parsed "+i+" offers.");
			OutputOffer offer = offers.get(i);
			
			//extend with category info 
			OfferScoring offerWScore = new OfferScoring(offer.getUrl(), offer.getNodeID(), offer.getIdentifiers(),
					offer.getDescProperties(),offer.getCluster_id(), categoryScoring, offer.getParentdescProperties(), offer.getPropertyToParent(), offer.getParentNodeID(), offer.getSpecTable(), title, description,specTables);
			
			//write meta info for the offer
			//writeOfferMetaInfo(offerWScore, writer);
		
			String clusterID = offerWScore.getCluster_id();
			if (!clustersCategorization.containsKey(clusterID)) {
				clustersCategorization.put(clusterID, new ClusterScoring());
			}
			clustersCategorization.get(clusterID).getOffersScoringInfo().add(offerWScore);
		}
		
//		writer.flush();
//		writer.close();
		
		System.out.println("Aggregate Category Info per Cluster");

		//now calculate dominant category per cluster
		for (ClusterScoring cs:clustersCategorization.values()) {
			cs.addCategoryInfo();
		}
		
		System.out.println("Write category info per cluster in file "+clustermetaFile.getPath());

		writeMetaClusters(clustersCategorization);
	}

	public void writeOfferMetaInfo(OfferScoring offer, BufferedWriter writer) throws IOException {
		
		JsonObject offer_item = new JsonObject();
		offer_item.addProperty("url", offer.getUrl());
		offer_item.addProperty("nodeID", offer.getNodeID());
		offer_item.addProperty("cluster_id", offer.getCluster_id());
		
		JsonArray id_array = new JsonArray();
		for (Map.Entry<String, HashSet<String>> ids : offer.getIdentifiers().entrySet()){
			JsonObject id = new JsonObject();
			id.addProperty(ids.getKey(), ids.getValue().toString());
			id_array.add(id);
		}
		
		JsonArray desc_array = new JsonArray();
		for (Map.Entry<String, HashSet<String>> desc : offer.getDescProperties().entrySet()){
			JsonObject d = new JsonObject();
			d.addProperty(desc.getKey(), desc.getValue().toString());
			desc_array.add(d);
		}
		
		
		offer_item.addProperty("dominant_category_offer", offer.getDominantCategoriesNames().toString());

		writer.write(offer_item.toString()+"\n");
		
	}

	private void writeMetaClusters(HashMap<String, ClusterScoring> clustersCategorization) throws IOException {
		
		BufferedReader reader = InputUtil.getBufferedReader(clustermetaFile);
		BufferedWriter writer = new BufferedWriter(new FileWriter(clustermetaUpdated));
		
		String line="";

		int clustercounter =0;
		
		while((line=reader.readLine())!=null) {
			JSONObject json = new JSONObject(line);
			
			String categoryName;
			double categDensity;
			
			if (!clustersCategorization.containsKey(json.get("id").toString())){
				continue;
			}
			clustercounter++;	
			
			if (clustersCategorization.get(json.get("id").toString()).getDominantCategory()==null) {
				categoryName = "not found";
				categDensity = 0.0;
			}
			else {
				categoryName =clustersCategorization.get(json.get("id").toString()).getDominantCategory().getCategory_name();
				categDensity = clustersCategorization.get(json.get("id").toString()).getCategoryDensity();
			}
			json.put("category", categoryName);
			json.put("categoryDensity", categDensity);
			
			json.put("cluster_size_in_offers", clustersCategorization.get(json.get("id").toString()).getOffersScoringInfo().size());
			
			writer.write(json.toString()+"\n");			
		}
		
		writer.flush();
		writer.close();
		System.out.println("Added cluster info to :"+clustercounter+" clusters");
	}
}
