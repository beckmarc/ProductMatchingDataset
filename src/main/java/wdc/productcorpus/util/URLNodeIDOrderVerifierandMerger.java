package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.json.JSONException;
import org.json.JSONObject;

import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;

public class URLNodeIDOrderVerifierandMerger {

	File inputDir = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\input");
	File outputFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\uniqueOffers");
	File keysFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\dublicateurlids.txt");
	
	public static void main (String args[]) throws JSONException, IOException{
		URLNodeIDOrderVerifierandMerger verifyOrder = new URLNodeIDOrderVerifierandMerger();
		if (args.length>0) {
			verifyOrder.inputDir = new File(args[0]);
			verifyOrder.outputFile = new File(args[1]);
			verifyOrder.keysFile = new File(args[2]);
		}
		
		HashSet<String> keys = verifyOrder.loadKeys();
		verifyOrder.checkURLorder(keys);
	}
	
	private HashSet<String> loadKeys() throws IOException {
		HashSet<String> keys = new HashSet<String>();
		
		BufferedReader reader =new BufferedReader(new FileReader(keysFile));
		String line="";
		while ((line=reader.readLine())!=null)
			keys.add(line);
		
		reader.close();
		return keys;
	}

	//one URL-nodeID should appear in exactly one file
	public void checkURLorder(HashSet<String> keys) throws JSONException, IOException {
		HashMap<String, ArrayList<OutputOffer>> offers= new HashMap<String, ArrayList<OutputOffer>>();
		DataImporter importOffer = new DataImporter();
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
		
		int duplicates =0;
		for (File f:inputDir.listFiles()) {
			System.out.println("Parsing "+f.getName());
			//BufferedReader reader = new BufferedReader(new FileReader(f));
			BufferedReader reader = new BufferedReader(new InputStreamReader
					(new GZIPInputStream(new FileInputStream(f)),
					"utf-8"));
			String line = "";
			while ((line=reader.readLine())!=null) {
				JSONObject json = new JSONObject(line);
				OutputOffer offer = importOffer.jsonToOffer(json);
				String key = offer.getUrl()+";"+offer.getNodeID();
				
				//if found in the keys it is a duplicate
				if (keys.contains(key)) {
					duplicates++;
					ArrayList<OutputOffer> exOffers = offers.get(key);
					if (exOffers==null) exOffers=new ArrayList<OutputOffer>();
					exOffers.add(offer);
					offers.put(key, exOffers);
				}
				else {
					String nodeID = offer.getNodeID();
					String url = offer.getUrl();
					offer.setNodeID(nodeID);
					offer.setUrl(url);

					writer.write(offer.toJSONObject(true)+"\n");
					
				}
			
				

			}
			reader.close();
		}
		
		System.out.println("Duplicate offers:"+duplicates);
		System.out.println("Calculating urls of multiple files");
		int counter = 0;
		
		
		for (Map.Entry<String, ArrayList<OutputOffer>> o:offers.entrySet()) {
			if(o.getValue().size()>1) {
				counter++;
				System.out.println("Merge for key: "+o.getKey());
				OutputOffer merged = mergeOffers(o.getValue());
				
				writer.write(merged.toJSONObject(true)+"\n");
			}
			
		}
		
		writer.flush();
		writer.close();
		
		System.out.println(counter +" URL-NodeID combinations are found in more than one file");
	}

	private OutputOffer mergeOffers(ArrayList<OutputOffer> offers) {
		OutputOffer mergedOffer = new OutputOffer();

		String nodeID = offers.get(0).getNodeID();
		String url = offers.get(0).getUrl();
		HashMap<String, HashSet<String>> mergedIdentifiers = new HashMap<String,HashSet<String>>();
		HashMap<String, HashSet<String>> mergedDescProps = new HashMap<String,HashSet<String>>();
		HashMap<String, HashSet<String>> mergedParentDescProps = new HashMap<String,HashSet<String>>();
		String propToParent=null;
		String parentNodeID=null;

		
		for (OutputOffer o:offers) {
			mergedIdentifiers = new HashMap<String,HashSet<String>>(mergemap(o.getNonprocessedIdentifiers(), mergedIdentifiers));
			mergedDescProps = new HashMap<String,HashSet<String>>( mergemap(o.getDescProperties(), mergedDescProps));
			mergedParentDescProps =new HashMap<String,HashSet<String>>(mergemap(o.getParentdescProperties(), mergedParentDescProps));
			
			if(!o.getParentNodeID().isEmpty()) parentNodeID = o.getParentNodeID();
			if (!o.getPropertyToParent().isEmpty()) propToParent = o.getPropertyToParent();
		}
		
		mergedOffer.setUrl(url);
		mergedOffer.setNodeID(nodeID);
		mergedOffer.setNonprocessedIdentifiers(mergedIdentifiers);
		mergedOffer.setDescProperties(mergedDescProps);
		mergedOffer.setParentdescProperties(mergedParentDescProps);
		mergedOffer.setParentNodeID(parentNodeID);
		mergedOffer.setPropertyToParent(propToParent);
		
		return mergedOffer;
	}

	private HashMap<String, HashSet<String>> mergemap(HashMap<String, HashSet<String>> small,
		HashMap<String, HashSet<String>> merged) {
		
		for (String v : small.keySet()) {
			HashSet<String> ex = merged.get(v);
			if (ex == null) ex = new HashSet<String>();
			ex.addAll(small.get(v));
			merged.put(v, ex);
					}
		return merged;
	}
}
