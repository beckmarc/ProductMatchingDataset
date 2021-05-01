package wdc.productcorpus.datacreator.Profiler.Categories;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.json.JSONObject;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import de.dwslab.dwslib.util.io.InputUtil;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;
import wdc.productcorpus.datacreator.Profiler.SpecTables.SpecTablesImporter;

public class CategorizerEvaluation {

	static File offersFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\cat_gs_offers.txt");
	static File lexiconFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\Lexicon");
	static File specTablesPath = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\SpecificationTables");

	static String outputPath = "C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization";
	static boolean rescale = false;
	static File gs = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\cat_gs_Evaluation.txt");
	
	static ArrayList<OfferScoring> predictedOffers = new ArrayList<>();
	
	private static boolean title = true;
	private static boolean description = true;
	private static boolean specTables = false;

	
	public static void main (String args[]) throws IOException {
		
		CategorizerEvaluation evaluate = new CategorizerEvaluation();
				
		//import the offer data
		DataImporter load = new DataImporter(offersFile);
		
		
		ArrayList<OutputOffer> offers = new ArrayList<OutputOffer>(load.importOffers());
		
		if (specTables) {
			//load offers with specification table data
			SpecTablesImporter loadTables = new SpecTablesImporter();
			offers = loadTables.addTableInfoToOffers(offers, specTablesPath);
		}
				
		//load the lexicons
		System.out.println("Import Lexicons");
		LexiconsImporter importLexicons = new LexiconsImporter(lexiconFile);
		ArrayList<CategoryScoring> categoryScoring = importLexicons.saveCategoryScoringInfo(rescale);
		
		//predict label
		evaluate.predict(offers, categoryScoring);
		
		evaluate.compare(predictedOffers, evaluate.loadGS(gs));
		
		//write meta info for the offer
		evaluate.writeOfferMetaInfo(predictedOffers, new File(outputPath+"\\offers_predictedLabels"));
		
		evaluate.writeErrorAnalysisLog();
	}
	
	private void writeErrorAnalysisLog() throws IOException {
		Map<Boolean, List<OfferScoring>> offersByLabel =
			    predictedOffers.stream().collect(Collectors.groupingBy(o -> o.isPredicted()));
		
		Map<String, List<OfferScoring>> wrongPredictedByLabel = offersByLabel.get(false).
				stream().collect(Collectors.groupingBy(o -> o.getCorrectLabel()));
		
		for (Map.Entry<String,List<OfferScoring>> offersPerCat : wrongPredictedByLabel.entrySet()) {
			writeOfferMetaInfo(offersPerCat.getValue(), new File(CategorizerEvaluation.outputPath+"\\"+offersPerCat.getKey()+"_wrongPredictions"));
		}
		
		
	}

	public void predict(ArrayList<OutputOffer> offers, ArrayList<CategoryScoring> categoryScoring){
		//get the dominant category for every offer		
		for (int i = 0; i< offers.size();i++) {
			if (offers.size()>1000 && i%1000==0) System.out.println("Parsed "+i+" offers.");
			OutputOffer offer = offers.get(i);
			

			//extend with category info 
			OfferScoring offerWScore = new OfferScoring(offer.getUrl(), offer.getNodeID(), offer.getIdentifiers(),
					offer.getDescProperties(),offer.getCluster_id(), categoryScoring, offer.getParentdescProperties(), offer.getPropertyToParent(), offer.getParentNodeID(),offer.getSpecTable(), title, description, specTables);
	
			predictedOffers.add(offerWScore);
		}
			
		System.out.printf("Parsed %d offers\n",offers.size());
	}
	
	public void writeOfferMetaInfo(List<OfferScoring> offers, File outputFile)  throws IOException {
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
		
		for (OfferScoring offer : offers) {
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
			offer_item.add("identifiers", id_array);
			
			JsonArray desc_array = new JsonArray();
			for (Map.Entry<String, HashSet<String>> desc : offer.getDescProperties().entrySet()){
				JsonObject d = new JsonObject();
				d.addProperty(desc.getKey(), desc.getValue().toString());
				desc_array.add(d);
			}
			offer_item.add("schema.org_description", desc_array);
			
			offer_item.addProperty("predicted_category", offer.getDominantCategoriesNames().toString());
			
			offer_item.addProperty("predicted", offer.isPredicted());
						
			offer_item.addProperty("correctLabel", offer.getCorrectLabel());

			writer.write(offer_item.toString()+"\n");
		}
		
		writer.flush();
		writer.close();
		
	}
	
	private void compare(ArrayList<OfferScoring> predictedOffers, ArrayList<OfferScoring> loadGS) {
		
		int correct = 0;
		int total = 0;
		
		for (int i=0; i<predictedOffers.size();i++) {
			for (int j=0; j<loadGS.size();j++) {
				if (((OutputOffer)predictedOffers.get(i)).equals((OutputOffer)loadGS.get(j))){
					total++;
					if(predictedOffers.get(i).getDominantCategoriesNames().contains(loadGS.get(j).getDominantCategoriesNames().get(0))){
						correct++;
						predictedOffers.get(i).setPredicted(true);					
					}
					predictedOffers.get(i).setCorrectLabel(loadGS.get(j).getDominantCategoriesNames().get(0));
					predictedOffers.get(i).setPredictedLabel(predictedOffers.get(i).getDominantCategoriesNames().toString());
				}
			}
		}
		
		System.out.println("Total Labels: "+total);
		System.out.println("Correct Labels: "+correct);
		System.out.println("Accuracy: "+((double)correct/(double)total));
	}

	public ArrayList<OfferScoring> loadGS(File gs) throws IOException {
		BufferedReader reader = InputUtil.getBufferedReader(gs);
		
		String line="";

		ArrayList<OfferScoring> labeledOffers = new ArrayList<>();
		
		while (reader.ready()) {
			line = reader.readLine();
			JSONObject json = new JSONObject(line);
			
			String url = json.getString("url");
			String nodeID = json.getString("nodeID");
			String category = json.getString("categoryLabel");
			
			OfferScoring labeledOffer = new OfferScoring(url, nodeID, category);
			
			labeledOffers.add(labeledOffer);
			
		}
		return labeledOffers;
	}
}
