package wdc.productcorpus.datacreator.Profiler.Categories;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;
import wdc.productcorpus.datacreator.Profiler.SpecTables.SpecTablesImporter;

public class FastTextFileCreator {
	
	private static boolean title = true;
	private static boolean description = true;
	private static boolean specTables = true;
	static String prefix = "__label__";
	static File offersFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\cat_gs_offers.txt");
	static File gs = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\cat_gs_Evaluation.txt");
	static File specTablesPath = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\SpecificationTables");
	
	static File output = new File ("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\fastTest_title_desc_spec_tables");
	
	public static void main(String args[]) throws IOException {
		
		BufferedWriter fastTextWriter = new BufferedWriter(new FileWriter(output));
		
		System.out.println("Import offers");
		DataImporter load = new DataImporter(offersFile);
		
		ArrayList<OutputOffer> offers = new ArrayList<OutputOffer>(load.importOffers());
		
		
		if (specTables) {
			System.out.println("Add specification information");
			//load offers with specification table data
			SpecTablesImporter loadTables = new SpecTablesImporter();
			offers = loadTables.addTableInfoToOffers(offers, specTablesPath);
		}
		
		//get the manual labels
		CategorizerEvaluation loadGS = new CategorizerEvaluation();
		
		HashMap<OutputOffer, String> GSasMap =  gstomap(loadGS.loadGS(gs));
		
		System.out.println("Create offer word vectors");
		//get the word vector for every offer
		for (OutputOffer offer: offers) {
			OfferScoring scoring = new OfferScoring(offer.getUrl(), offer.getNodeID(), offer.getIdentifiers(),
					offer.getDescProperties(),offer.getCluster_id(), 
					 offer.getParentdescProperties(), offer.getPropertyToParent(), offer.getParentNodeID(),offer.getSpecTable(), title, description, specTables);
		
			HashSet<String> wordVector = scoring.getWords();
			
			fastTextWriter.write(prefix+GSasMap.get(offer)+"\t"+String.join(" ", wordVector)+"\n");
			
		}
		
		fastTextWriter.flush();
		fastTextWriter.close();
	}

	private static HashMap<OutputOffer, String> gstomap(ArrayList<OfferScoring> labels_gs) {
		
		HashMap<OutputOffer, String> gstomap = new HashMap<>();
		for (OfferScoring s: labels_gs){
			OutputOffer offer = new OutputOffer(s.getUrl(), s.getNodeID());
			gstomap.put(offer, s.getCorrectLabel());
		}
		return gstomap;
	}
}
