package wdc.productcorpus.v2.datacreator.ListingsAds;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.PositiveScoresOnlyCollector;
import org.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.dwslab.dwslib.framework.Processor;
import de.uni_mannheim.informatik.dws.winter.utils.query.P.IsContainedIn;
import wdc.productcorpus.datacreator.Extractor.SupervisedNode;
import wdc.productcorpus.datacreator.ListingsAds.EvaluateListingDetection;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;
import wdc.productcorpus.util.DomainUtil;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.util.StDevStats;
import wdc.productcorpus.v2.datacreator.filter.MainEntityFilter;
import wdc.productcorpus.v2.datacreator.filter.VariationDetectFilter;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;

/**
 * Evaluates any given heuristic for the Entity format.
 * Needs the labeled negative pairs and determines the positive pairs with it. 
 * <br><br>
 * Inputs:<br>
 * arg0: Path to the file with the positive pairs/entities<br>
 * arg1: Path to the file with the negative pairs/entities<br>
 * arg2: Path to the file with the labeled negative pairs/entities<br>
 * arg3: Path to the folder where the output file will be written to<br>
 * 
 * @author Marc Becker
 *
 */
public class EvaluateHeuristicV2 {
		
	static ArrayList<Entity> positives = new ArrayList<Entity>();
	static ArrayList<Entity> negatives = new ArrayList<Entity>();
	static ArrayList<Entity> labeledPositives = new ArrayList<Entity>();
	static ArrayList<Entity> labeledNegatives = new ArrayList<Entity>();
	
	public static void main (String args[]) {
		
		if (args.length!=0) {
			readLines(new File(args[1]));
			switch(args[0]) {
				case "listing":
					listingDetection();
					break;
				case "variation":
					variationsDetection();
					break;
				case "mainEntity":
					mainEntityDetection();
					break;
			}
		}
		try {
			PrintUtils.p("Positive Entities: " + positives.size());
			PrintUtils.p("Negative Entities: " + negatives.size());
			PrintUtils.p("Labeled Positive Entities: " + labeledPositives.size());
			PrintUtils.p("Labeled Negative Entities: " + labeledNegatives.size());
		}
		catch (Exception e){
			System.out.println(e.getMessage());
		}
		
		
		ArrayList<Entity> truePositives_ = new ArrayList<Entity>();
		ArrayList<Entity> trueNegatives_ = new ArrayList<Entity>();
		ArrayList<Entity> falsePositives_ = new ArrayList<Entity>();
		ArrayList<Entity> falseNegatives_ = new ArrayList<Entity>();
		
		System.out.println("Starting Evaluation...");
		
		int truePositives = 0;
		int falsePositives = 0;
		int falseNegatives = 0;
		int trueNegatives = 0;
		
		for(Entity e : positives) {
			if(EntityStatic.isContained(e, labeledPositives)) { // Correct classification -> correctly labeled as positive (TP)
				truePositives++;
				truePositives_.add(e);
			}
			else if(EntityStatic.isContained(e, labeledNegatives)){ // Wrong classification -> erroneously labeled as negative (FN)
				falseNegatives++;
				falseNegatives_.add(e);
			}
		}
		
		for(Entity e : negatives) {
			if(EntityStatic.isContained(e, labeledNegatives)) { // Correct classification -> correctly labeled as negative (TN)
				trueNegatives++;
				trueNegatives_.add(e);
			}
			else if(EntityStatic.isContained(e, labeledPositives)) { // Wrong classification -> erroneously labeled as positive (FP)
				falsePositives++;
				falsePositives_.add(e);
			}
		}
		
		
		double precision =  (double) truePositives/ (truePositives+falsePositives);
		double recall = (double) truePositives/ (truePositives+falseNegatives);
		double f1 = (2*precision*recall)/(precision+recall);
		
		System.out.println("\n---------------Pairs-----------------");
		System.out.printf("True Positives: %d \nTrue Negatives: %d \nFalse Positives: %d \nFalse Negatives: %d \n",
				truePositives, trueNegatives, falsePositives, falseNegatives);
	
		System.out.println("\n----------Listing and Ads Prediction Evaluation--------------");
		System.out.println("Items in GS: "+(truePositives+falseNegatives+falsePositives+trueNegatives));
		System.out.printf("Precision: %f \nRecall: %f \nF1: %f \n", precision, recall, f1);
		
		System.out.println("\n---------------------------------------------------------");
		if(args[1] != null) {
			File outputFile = new File(args[2]);
			System.out.println("Writing results to directory: " + outputFile.getPath());
			try {
				CustomFileWriter.writeLineToFile("result", outputFile, "", "---------------------------------------TRUE-POSITIVES---------------------------------------");
				CustomFileWriter.writeEntitiesToFile("result", outputFile, "", truePositives_);
				CustomFileWriter.writeLineToFile("result", outputFile, "", "---------------------------------------TRUE-NEGATIVES---------------------------------------");
				CustomFileWriter.writeEntitiesToFile("result", outputFile, "", trueNegatives_);
				CustomFileWriter.writeLineToFile("result", outputFile, "", "---------------------------------------FALSE-POSITIVES---------------------------------------");
				CustomFileWriter.writeEntitiesToFile("result", outputFile, "", falsePositives_);
				CustomFileWriter.writeLineToFile("result", outputFile, "", "---------------------------------------FALSE-NEGATIVES---------------------------------------");
				CustomFileWriter.writeEntitiesToFile("result", outputFile, "", falseNegatives_);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		} 
		else {
			PrintUtils.p("Could not write results. No path for file is given. Please ad the path as the second argument.");
		}
		
	}
	
	private static void mainEntityDetection() {
		MainEntityFilter mef = new MainEntityFilter();
		ArrayList<Entity> entities = new ArrayList<Entity>();
		entities.addAll(positives);
		entities.addAll(negatives);
		String currentUrl = entities.get(0).url;
		ArrayList<Entity> entitiesUrl = new ArrayList<Entity>();
		for(Entity e : entities) {
			if(!currentUrl.equals(e.url)) {
				if(mef.checkForMainProduct(entitiesUrl).size() == 1) {
					labeledPositives.addAll(entitiesUrl);
				}
				else {
					labeledNegatives.addAll(entitiesUrl);
				}
				entitiesUrl.clear();
			}
			entitiesUrl.add(e);
			currentUrl = e.url;
		}
		// process once more for last entities
		if(mef.checkForMainProduct(entitiesUrl).size() == 1) {
			labeledPositives.addAll(entitiesUrl);
		}
		else {
			labeledNegatives.addAll(entitiesUrl);
		}
	}
	
	
	
	private static void variationsDetection() {
		VariationDetectFilter vdf = new VariationDetectFilter();
		ArrayList<Entity> entities = new ArrayList<Entity>();
		entities.addAll(positives);
		entities.addAll(negatives);
		String currentUrl = entities.get(0).url;
		ArrayList<Entity> entitiesUrl = new ArrayList<Entity>();
		for(Entity e : entities) {
			if(!currentUrl.equals(e.url)) {
				if(vdf.isVariation(entitiesUrl)) {
					labeledPositives.addAll(entitiesUrl);
				}
				else {
					labeledNegatives.addAll(entitiesUrl);
				}
				entitiesUrl.clear();
			}
			entitiesUrl.add(e);
			currentUrl = e.url;
		}
		// process once more for last entities
		if(vdf.isVariation(entitiesUrl)) {
			labeledPositives.addAll(entitiesUrl);
		}
		else {
			labeledNegatives.addAll(entitiesUrl);
		}
	}
	
	private static void listingDetection() {
		DetectListingsAds dla = new DetectListingsAds();
		ArrayList<Entity> entities = new ArrayList<Entity>();
		entities.addAll(positives);
		entities.addAll(negatives);
		
		String currentUrl = entities.get(0).url;
		ArrayList<Entity> entitiesUrl = new ArrayList<Entity>();
		for(Entity e : entities) {
			if(!currentUrl.equals(e.url)) {
				if(dla.isListingPage(entitiesUrl)) {
					labeledPositives.addAll(entitiesUrl);
				}
				else {
					labeledNegatives.addAll(entitiesUrl);
				}
				entitiesUrl.clear();
			}
			entitiesUrl.add(e);
			currentUrl = e.url;
		}
		// process once more for last entities
		if(dla.isListingPage(entitiesUrl)) {
			labeledPositives.addAll(entitiesUrl);
		}
		else {
			labeledNegatives.addAll(entitiesUrl);
		}
	}

	private static void readLines(File file) {
		String line;
		try {
			BufferedReader br = InputUtil.getBufferedReader(file);
			while ((line = br.readLine()) != null) {
				if(line.startsWith("true")) {
					positives.add(EntityStatic.parseEntity(line.substring(4)));
				} else if(line.startsWith("false")) {
					negatives.add(EntityStatic.parseEntity(line.substring(5)));
				}
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	

}
