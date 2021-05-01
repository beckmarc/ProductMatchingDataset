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

public class SanityCheckOldNew {

	File newCorpus = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\offers_clean.txt_filteredIDLabels.txt");
	File trainingSetDir = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\input");
	File sanityLogDir = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp");
	
	public static void main(String[] args) throws IOException {
		SanityCheckOldNew check = new SanityCheckOldNew();
		
		if (args.length>0){
			check.newCorpus = new File(args[0]);
			check.trainingSetDir = new File(args[1]);
			check.sanityLogDir = new File(args[2]);
		}

		check.check();
	}

	public void check() throws IOException {
		
		DataImporter importOffers = new DataImporter(newCorpus);
		HashSet<OutputOffer> newOffers = importOffers.importOffers();
		HashMap<OutputOffer, OutputOffer> newOffersMAP = new HashMap<OutputOffer,OutputOffer>();
		
		for (OutputOffer o: newOffers) {
			newOffersMAP.put(o, o);
		}
		
		for (File f:trainingSetDir.listFiles()) {
			
			BufferedWriter writer = new BufferedWriter(new FileWriter(sanityLogDir+"/"+f.getName()+"_sanityCheck"));
			
			HashSet<OutputOffer> nonExistinginNew = new HashSet<OutputOffer>();
			
			ArrayList<String> positivesInconsistent = new ArrayList<String>();
			ArrayList<String> negativesInconsistent = new ArrayList<String>();
			int cannotVerifyLabel = 0;
			
			//now parse the training file
			BufferedReader reader = new BufferedReader(new FileReader(f));
			String line="";
			while ((line=reader.readLine())!=null) {
				
				String[] lineParts = line.split("#####");
				String pair1 = lineParts[0];
				String pair1_nodeID = pair1.split("\\s")[0];
				String pair1_url = pair1.split("\\s")[1];
				OutputOffer o1 = new OutputOffer(pair1_url, pair1_nodeID);
				
				String pair2 = lineParts[1];
				String pair2_nodeID = pair2.split("\\s")[0];
				String pair2_url = pair2.split("\\s")[1];
				OutputOffer o2 = new OutputOffer(pair2_url, pair2_nodeID);
				
				String matching_ = lineParts[2];
				Boolean matching = false;
				if (matching_.equals("1")) matching=true;

				OutputOffer o1_old = newOffersMAP.get(o1);
				OutputOffer o2_old = newOffersMAP.get(o2);

				if (o1_old == null) nonExistinginNew.add(o1);
				if (o2_old == null) nonExistinginNew.add(o2);
				
				if ((o1_old == null) || (o2_old == null)) cannotVerifyLabel++;
				else {
					if (matching) {
						if (!o1_old.getCluster_id().equals(o2_old.getCluster_id()))
							positivesInconsistent.add(line);
					}
					else {
						if (o1_old.getCluster_id().equals(o2_old.getCluster_id()))
							negativesInconsistent.add(line);
					}
					
				}
		
			}
			
			reader.close();
			
			writer.write("Not existing offers in the new Training Set \n");
			for (OutputOffer o:nonExistinginNew) {
				writer.write(o.toJSONObject(true)+"\n");
			}
			
			writer.write("Inconsistent positives"+"\n");
			for (String p:positivesInconsistent)
				writer.write(p+"\n");
			
			writer.write("Inconsistent negatives \n");
			for (String n:negativesInconsistent) {
				writer.write(n+"\n");
			}
			
			writer.write("Not existing offers : "+nonExistinginNew.size()+"\n");
			writer.write("Positive inconsistent : "+positivesInconsistent.size()+"\n");
			writer.write("Negative inconsistent: "+negativesInconsistent.size()+"\n");
			writer.write("Cannot verify labels for: "+cannotVerifyLabel+"\n");
			
			writer.flush();
			writer.close();
		}
		
		
	}
}
