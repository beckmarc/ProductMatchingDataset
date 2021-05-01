package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;

public class URLNodeIDOrderVerifier {

	File inputDir = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp");
	
	public static void main (String args[]) throws JSONException, IOException{
		URLNodeIDOrderVerifier verifyOrder = new URLNodeIDOrderVerifier();
		if (args.length>0) {
			verifyOrder.inputDir = new File(args[0]);
		}
		
		verifyOrder.checkURLorder();
	}
	
	//one URL-nodeID should appear in exactly one file
	public void checkURLorder() throws JSONException, IOException {
		HashMap<String, HashSet<String>> offersToFiles= new HashMap<String, HashSet<String>>();
		
		for (File f:inputDir.listFiles()) {
			System.out.println("Parsing "+f.getName());
			BufferedReader reader = new BufferedReader(new FileReader(f));
			String line = "";
			while ((line=reader.readLine())!=null) {
				JSONObject json = new JSONObject(line);
				String nodeID = json.get("url").toString();
				String url = json.get("nodeID").toString();
				String key= nodeID+";"+url;
			
				HashSet<String> exFiles = offersToFiles.get(key);
				if (exFiles==null) exFiles=new HashSet<String>();
				exFiles.add(f.getName());
				offersToFiles.put(key, exFiles);

			}
			reader.close();
		}
		
		System.out.println("Calculating urls of multiple files");
		int counter = 0;
		
		
		for (Map.Entry<String, HashSet<String>> o:offersToFiles.entrySet()) {
			if(o.getValue().size()>1) {
				counter++;
				System.out.println(o.getKey());
				
			}
			
		}
		
	
		System.out.println(counter +" URL-NodeID combinations are found in more than one file");
	}
}
