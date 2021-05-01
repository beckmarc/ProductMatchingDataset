package wdc.productcorpus.datacreator.ListingsAds;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;


public class EvaluateListingDetection {
	
	static File positives = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\positives");
	static File negatives = new File ("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\negatives");
	static File gs = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\GS_ListingPages.txt");
	static boolean log = true;

	public static void main (String args[]) {
		
		if (args.length!=0) {
			positives = new File (args[0]);
			negatives = new File (args[1]);
			gs = new File(args[2]);
			log = Boolean.valueOf(args[3]);
		}
		try {
			EvaluateListingDetection evaluator = new EvaluateListingDetection();
			HashMap<OutputOffer, String> gs = evaluator.loadGS();
			HashMap<OutputOffer, String> offers = evaluator.loadPredictedOffers();
			evaluator.evaluate(gs, offers);
		}
		catch (Exception e){
			System.out.println(e.getMessage());
		}
	}
	
	public HashMap<OutputOffer, String> loadGS() throws IOException {
		
		System.out.println("Load GS...");
		
		DataImporter importOffer = new DataImporter();
		
		HashMap<OutputOffer, String> gsOffers = new HashMap<>();
		
		BufferedReader reader = new BufferedReader(new FileReader(gs));
		String line = "";
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("TRUE")) {
				JSONObject offer = new JSONObject(line.replace("TRUE", ""));
				gsOffers.put(importOffer.jsonToOffer(offer),"TRUE");
			}
			else if (line.startsWith("FALSE")) {
				JSONObject offer = new JSONObject(line.replace("FALSE", ""));
				gsOffers.put(importOffer.jsonToOffer(offer),"FALSE");
			}
			else {
				System.out.println("No label in GS. Please check the file.");
				System.exit(1);
			}
		}
		
		reader.close();
		
		return gsOffers;
	}
	
	public HashMap<OutputOffer, String> loadPredictedOffers() throws IOException {
		System.out.println("Load offers...");
		
		DataImporter importOffer = new DataImporter();
		
		HashMap<OutputOffer, String> offers = new HashMap<>();

		
		//load predicted positives
		BufferedReader reader = new BufferedReader(new FileReader(positives));
		String line = "";
		while ((line = reader.readLine()) != null) {
			JSONObject offer = new JSONObject(line);
			offers.put(importOffer.jsonToOffer(offer),"TRUE");
		}
		reader.close();
		
		//load predicted negatives
		reader = new BufferedReader(new FileReader(negatives));
		while ((line = reader.readLine()) != null) {
			JSONObject offer = new JSONObject(line);
			offers.put(importOffer.jsonToOffer(offer),"FALSE");
		}
		reader.close();
		
		return offers;
	}
	
	public void evaluate(HashMap<OutputOffer, String> gs, HashMap<OutputOffer, String> offers) {
		
		System.out.println("Evaluate...");
		
		int truePositives = 0;
		int falsePositives = 0;
		int falseNegatives = 0;
		int trueNegatives = 0;
		
		HashMap<OutputOffer, String> proffers_intersection = new HashMap<OutputOffer, String>();
		for (OutputOffer gsOffer: gs.keySet()) {
			if(!offers.containsKey(gsOffer)) {
				System.out.println("Could not find offer: "+gsOffer.getNodeID()+"_"+gsOffer.getUrl());
			}
			proffers_intersection.put(gsOffer, offers.get(gsOffer));
		}
		
		ArrayList<OutputOffer> gs_true = new ArrayList<OutputOffer>();
		ArrayList<OutputOffer> gs_false = new ArrayList<OutputOffer>();
		
		for (Map.Entry<OutputOffer,String> gsOffer: gs.entrySet()) {
			if (gsOffer.getValue().equals("TRUE")) gs_true.add(gsOffer.getKey());
			else gs_false.add(gsOffer.getKey());
		}
		
		System.out.println("GS_TRUE elements: "+gs_true.size());
		System.out.println("GS_FALSE elements: "+gs_false.size());
				
		ArrayList<OutputOffer> predicted_true = new ArrayList<OutputOffer>();
		ArrayList<OutputOffer> predicted_false = new ArrayList<OutputOffer>();

		for (Map.Entry<OutputOffer,String> predictedOffer: proffers_intersection.entrySet()) {
			if (predictedOffer.getValue().equals("TRUE")) predicted_true.add(predictedOffer.getKey());
			else predicted_false.add(predictedOffer.getKey());
		}
		
		System.out.println("Predicted True elements: "+predicted_true.size());
		System.out.println("Predicted False elements: "+predicted_false.size());
		
		//evaluate positive predictions
		ArrayList<OutputOffer> truePositiveOffers = new ArrayList<>(gs_true);
		truePositiveOffers.retainAll(predicted_true);
		truePositives = truePositiveOffers.size();
		
		ArrayList<OutputOffer> falseNegativeOffers = new ArrayList<>(gs_true);
		falseNegativeOffers.retainAll(predicted_false);
		falseNegatives = falseNegativeOffers.size();
		
		if (log) {
			System.out.println("-------- Log false negatives --------");
			for (OutputOffer fn:falseNegativeOffers)
				System.out.println(fn.toJSONObject(true));
		}
		
		//evaluate negative predictions
		ArrayList<OutputOffer> falsePositiveOffers = new ArrayList<>(gs_false);
		falsePositiveOffers.retainAll(predicted_true);
		falsePositives = falsePositiveOffers.size();
		
		if (log) {
			System.out.println("-------- Log false positives --------");

			for (OutputOffer fp: falsePositiveOffers)
				System.out.println(fp.toJSONObject(true));
		}
		ArrayList<OutputOffer> trueNegativeOffers = new ArrayList<>(gs_false);
		trueNegativeOffers.retainAll(predicted_false);
		trueNegatives = trueNegativeOffers.size();
		
		double precision =  (double) truePositives/ (truePositives+falsePositives);
		double recall = (double) truePositives/ (truePositives+falseNegatives);
		double f1 = (2*precision*recall)/(precision+recall);
		
		System.out.println("---Listing and Ads Prediction Evaluation---");
		System.out.println("Items in GS: "+(truePositives+falseNegatives+falsePositives+trueNegatives));
		System.out.printf("Precision: %f \nRecall: %f \nF1: %f \n", precision, recall, f1);
		
	}
}
