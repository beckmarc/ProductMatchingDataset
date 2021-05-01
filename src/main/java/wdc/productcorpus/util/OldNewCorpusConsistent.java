package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;

public class OldNewCorpusConsistent {
	
	File newclusters = new File ("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\consistency\\clusters");
	File gs = new File ("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\consistency\\offers_gs");
	File newCorpus = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\consistency\\offers_newCorpus");

	File output_gsOffers = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\consistency\\gsOffers_UPDATED");
	File output_gsOffers_toadd = new File ("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\consistency\\gsOffers_UPDATED_missing");
	
	
	public static void main(String args[]) throws JSONException, IOException {
		OldNewCorpusConsistent update = new OldNewCorpusConsistent();
		
		if(args.length>0) {
			update.newclusters = new File(args[0]);
			update.gs = new File(args[1]);
			update.newCorpus = new File(args[2]);
			update.output_gsOffers = new File(args[3]);
			update.output_gsOffers_toadd = new File(args[4]);
		}
		update.updateClusterIDs();
	}
	
	public void updateClusterIDs() throws JSONException, IOException {
				
		//read new clusters and create index
		System.out.println("Create cluster index of new corpus");
		HashMap<String,Integer> newClusters_idx = readIndex(newclusters);
		
		//import gs offers
		System.out.println("Import offers from gs");
		DataImporter importOffer = new DataImporter(gs);
		HashSet<OutputOffer> gsOffers = importOffer.importOffers();
		
		HashSet<OutputOffer> new_gsOffers = new HashSet<OutputOffer>();
		
		System.out.println("Align cluster-ids in the GS");
		for (OutputOffer offer:gsOffers) {
			Integer cluster_id = null;

			for (Map.Entry<String, HashSet<String>> offer_ids : offer.getIdentifiers().entrySet()) {
				for (String id_value : offer_ids.getValue()) {
					if (null == cluster_id) cluster_id= newClusters_idx.get(id_value);
					else {
						if (newClusters_idx.containsKey(id_value) && !newClusters_idx.get(id_value).equals(cluster_id)) System.out.println("Ids of same offer in different clusters:"+offer.toJSONObject(true).toString());
					}
				}
			}
			if (null==cluster_id) System.out.println("No cluster found for: "+offer.toJSONObject(true).toString());
			else {
				OutputOffer offer_new = new OutputOffer(offer.getUrl(),offer.getNodeID(),offer.getIdentifiers(),offer.getDescProperties(), cluster_id.toString(),
						offer.getParentdescProperties(),offer.getPropertyToParent(),offer.getParentNodeID(),offer.getSpecTable());
				
				new_gsOffers.add(offer_new);
			}
				
			
		}
		
		System.out.println("Write new GS entities");
		BufferedWriter writer = new BufferedWriter(new FileWriter(output_gsOffers));
		for (OutputOffer newOffer: new_gsOffers) {
			writer.write(newOffer.toJSONObject(true)+"\n");		
		}
		writer.flush();
		writer.close();
		
		System.out.println("Find missing GS entities");
		System.out.println("First load all new entities");
		importOffer = new DataImporter(newCorpus);
		HashSet<OutputOffer> corpusOffers = importOffer.importOffers();
		
		System.out.println("Write missing entities");
		BufferedWriter writer_missing = new BufferedWriter(new FileWriter(output_gsOffers_toadd));
		for (OutputOffer newOffer:new_gsOffers) {
			if (!corpusOffers.contains(newOffer)) writer_missing.write(newOffer.toJSONObject(true)+"\n");
		}
		writer_missing.flush();
		writer_missing.close();
		
		System.out.println("DONE");
	}

	private HashMap<String, Integer> readIndex(File clusters) throws JSONException, IOException {
		BufferedReader reader = new BufferedReader(new FileReader(clusters));
		String line = "";
		
		HashMap<String,Integer> index = new HashMap<String,Integer>();
		
		while((line=reader.readLine())!=null) {
			JSONObject json = new JSONObject(line);
			Integer clusterID = json.getInt("id");
			String [] values = json.getString("id_values").toString().replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\\s","").split(",");
			for (int i=0;i<values.length;i++)
				index.put(values[i], clusterID);
		}
		reader.close();
		return index;
	}

	
}
