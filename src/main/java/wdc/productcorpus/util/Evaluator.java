package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;

public class Evaluator {
	static File entitiesFile = new File("C:\\Users\\User\\Google Drive\\ISWC_ClusterQualityEvaluation\\samplesEntities\\Office_Products_largesample.txt_completeEntities");
	static File samplesFile = new File("C:\\Users\\User\\Google Drive\\ISWC_ClusterQualityEvaluation\\samples\\Office_Products_largesample.txt_PROCESS");
	
	public static void main (String []args) throws IOException{
		
		
		DataImporter importEntities = new DataImporter(entitiesFile);
		HashSet<OutputOffer> entities = importEntities.importOffers();
		HashMap<OutputOffer,OutputOffer> entitiesMap = new HashMap<OutputOffer,OutputOffer>();
		
		for (OutputOffer o:entities) {
			entitiesMap.put(o, o);
		}
		
		BufferedReader reader = new BufferedReader(new FileReader(samplesFile));
		String line ="";
		
		System.out.println("Press y for the next entry");
		Scanner scanner = new Scanner(System.in);
		String nextPair = scanner.nextLine();
		
		while ((line=reader.readLine())!=null && nextPair.equals("y")) {
			
			String[] lineParts = line.split(";_:node|;<");
			String pair1 = lineParts[1];
			String pair1_nodeID="";
			if (pair1.startsWith("http")) pair1_nodeID = "<"+pair1.split("\\|")[0];
			else pair1_nodeID = "_:node"+pair1.split("\\|")[0];
			
			String pair1_url = pair1.split("\\|")[1];
			OutputOffer o1 = new OutputOffer(pair1_url, pair1_nodeID);
			
			String pair2 = lineParts[2];
			String pair2_nodeID = "";
			if (pair2.startsWith("http")) pair2_nodeID = "<"+ pair2.split("\\|")[0];
			else pair2_nodeID = "_:node"+pair2.split("\\|")[0];
			String pair2_url = pair2.split("\\|")[1];
			OutputOffer o2 = new OutputOffer(pair2_url, pair2_nodeID);
			

			OutputOffer o1_complete = entitiesMap.get(o1);
			OutputOffer o2_complete = entitiesMap.get(o2);

			System.out.println("Offer1: "+o1_complete.toJSONObject(true));
			System.out.println("Offer2: "+o2_complete.toJSONObject(true));

			nextPair = scanner.nextLine();
			
			if (nextPair.equals("id")) {
				System.out.println(pair1_nodeID+"|"+pair1_url+";"+pair2_nodeID+"|"+pair2_url);
				nextPair = scanner.nextLine();
			}
		}
		
		scanner.close();
		reader.close();
	}
}
