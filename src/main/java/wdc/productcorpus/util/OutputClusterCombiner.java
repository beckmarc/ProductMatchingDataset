package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;

public class OutputClusterCombiner {

	File offersClusters = new File("");
	File offersDir = new File("");
	File offersOutputFile = new File("");
	
	public static void main(String args[]) throws IOException {
		
		OutputClusterCombiner combineFiles = new OutputClusterCombiner();
		
		if (args.length>0) {
			combineFiles.offersClusters= new File(args[0]);
			combineFiles.offersDir=new File(args[1]);
			combineFiles.offersOutputFile = new File(args[2]);
		}
		HashMap<String,String> clusterindex = combineFiles.offersToClusters();
		
		combineFiles.createCompleteOffers(clusterindex);
	}
	
	
	public HashMap<String,String> offersToClusters () throws IOException {
		
		HashMap<String,String> offersIndex = new HashMap<String,String>();
		BufferedReader reader = new BufferedReader(new FileReader(offersClusters));
		
		String line ="";
		
		while ((line = reader.readLine())!=null) {
			
			JSONObject json = new JSONObject(line);

			String url = json.getString("url");
			String nodeID = json.getString("nodeID");
			String clusterID = json.get("cluster_id").toString();
			
			String key = url+nodeID;
			if (offersIndex.containsKey(key)) {
				System.out.println("Offers "+key+" is in multiple clusters");
				System.exit(1);
			}
			
			offersIndex.put(key, clusterID);
		}
		
		reader.close();
		
		System.out.println("Created cluster index");
		return offersIndex;
	}
	
	public void createCompleteOffers(HashMap<String,String> offersIndex) throws JSONException, IOException{
		
		DataImporter importOffer = new DataImporter();
		ArrayList<OutputOffer> completeOffers = new ArrayList<OutputOffer>();
		
		for (File f: offersDir.listFiles()) {
			BufferedReader reader = new BufferedReader(new FileReader(f));
			String line ="";
			
			while ((line = reader.readLine())!=null) {
				
				JSONObject json = new JSONObject(line);

				OutputOffer offer = importOffer.jsonToOffer(json);
				String clusterID = offersIndex.get(offer.getUrl()+offer.getNodeID());
				if (null == clusterID) continue;
				
				else offer.setCluster_id(clusterID);
				
				completeOffers.add(offer);
			}
			
			reader.close();
		}
		
		System.out.println("Complete Offers");
		//write complete offers
		BufferedWriter writer = new BufferedWriter (new FileWriter(this.offersOutputFile));
		for (OutputOffer o: completeOffers) {
			writer.write(o.toJSONObject(true)+"\n");
		}
		
		writer.flush();
		
		writer.close();
		
		System.out.println("DONE");
	}
}
