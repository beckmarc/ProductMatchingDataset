package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;

public class EntitiesRetrieverWFilter {

	File corpus = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\offers_clean.txt_filteredIDLabels.txt");
	File subset = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\input");
	File outputDir = new File ("");
	
	public static void main(String[] args) throws IOException {
		EntitiesRetrieverWFilter check = new EntitiesRetrieverWFilter();
		
		if (args.length>0){
			check.corpus = new File(args[0]);
			check.subset = new File(args[1]);
			check.outputDir = new File(args[2]);
		}

		check.check();
	}

	public void check() throws IOException {
		
		DataImporter importOffers = new DataImporter(corpus);
		HashSet<OutputOffer> newOffers = importOffers.importOffers();
		HashMap<OutputOffer, OutputOffer> newOffersMAP = new HashMap<OutputOffer,OutputOffer>();
		
		for (OutputOffer o: newOffers) {
			newOffersMAP.put(o, o);
		}
		
		for (File f:subset.listFiles()) {
			
			System.out.println("Filtering entities of file: "+f.getName());
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir+"/"+f.getName()+"_completeEntities"));
			
			HashSet<OutputOffer> offersToWrite = new HashSet<OutputOffer>();
		
			//now parse the training file
			BufferedReader reader = new BufferedReader(new FileReader(f));
			String line="";
			while ((line=reader.readLine())!=null) {
				try{
//					//for evaluating purposes
//					String[] lineParts = line.split(";_:node|;<");
//					String pair1 = lineParts[1];
//					String pair1_nodeID="";
//					if (pair1.startsWith("http")) pair1_nodeID = "<"+pair1.split("\\|")[0];
//					else pair1_nodeID = "_:node"+pair1.split("\\|")[0];
//					
//					String pair1_url = pair1.split("\\|")[1];
//					OutputOffer o1 = new OutputOffer(pair1_url, pair1_nodeID);
//					
//					String pair2 = lineParts[2];
//					String pair2_nodeID = "";
//					if (pair2.startsWith("http")) pair2_nodeID = "<"+ pair2.split("\\|")[0];
//					else pair2_nodeID = "_:node"+pair2.split("\\|")[0];
//					String pair2_url = pair2.split("\\|")[1];
//					OutputOffer o2 = new OutputOffer(pair2_url, pair2_nodeID);
					
					
					//from Ralph's gs
					String[] lineParts = line.split("#####");
					String pair1_nodeID=lineParts[0].split(" ")[0];					
					String pair1_url = lineParts[0].split(" ")[1];

					String pair2_nodeID = lineParts[1].split(" ")[0];
					String pair2_url = lineParts[1].split(" ")[1];
					
					OutputOffer o1 = new OutputOffer(pair1_url, pair1_nodeID);
					OutputOffer o2 = new OutputOffer(pair2_url, pair2_nodeID);

					OutputOffer o1_old = newOffersMAP.get(o1);
					OutputOffer o2_old = newOffersMAP.get(o2);

					if (o1_old == null) System.out.println("Cannot find: "+pair1_url+pair1_nodeID);
					if (o2_old == null) System.out.println("Cannot find: "+pair2_url+pair2_nodeID);

					offersToWrite.add(o1_old);
					offersToWrite.add(o2_old);
				}
				catch (Exception e){
					System.out.println(e.getMessage());
					System.out.println("Could not parse line "+line);
					System.exit(0);
				}
				
		
			}
			
			reader.close();
			
			for (OutputOffer o:offersToWrite) {
				writer.write(o.toJSONObject(true)+"\n");
			}

			writer.flush();
			writer.close();
		}
		
		
	}
}
