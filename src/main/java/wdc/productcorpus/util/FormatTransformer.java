package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;

public class FormatTransformer {

	File inputFileDir = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Index\\input");
	File transformedFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\transform");
	
	
	public static void main(String args[]) throws IOException {
		
		FormatTransformer transform = new FormatTransformer();
		
		if(args.length>0) {
			transform.inputFileDir = new File (args[0]);
			transform.transformedFile = new File (args[1]);
		}
		
		transform.transformFromTabToJSON();
	}
	
	
	public void transformFromTabToJSON() throws IOException{
		
		HashMap<String, OutputOffer> offers = new HashMap<String, OutputOffer>();
		
		for (File inputFile:inputFileDir.listFiles()) {
			
			System.out.println("Parsing "+inputFile);
			
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			String line = "";
			while ((line = reader.readLine())!= null) {
				String [] lineparts = line.split("\\t");
				String nodeID = lineparts[1];
				String url = lineparts[2].replace("<","").replace(">", "");
				String idProp = lineparts[3];
				String idvalue = lineparts[4];
				OutputOffer offer;
				
				if (offers.containsKey(nodeID+url)) {
					offer = offers.get(nodeID+url);
					HashMap<String, HashSet<String>> identifiers  = offer.getIdentifiers();
					HashSet<String> exValues = identifiers.get(idProp);
					if (null == exValues) exValues= new HashSet<String>();
					
					exValues.add(idvalue);
					identifiers.put(idProp, exValues);
					offer.setIdentifiers(identifiers);
				}
				else {
					offer = new OutputOffer();
					
					HashSet<String> idValues = new HashSet<String>();
					idValues.add(idvalue);
					HashMap<String,HashSet<String>> identifiers = new HashMap<String, HashSet<String>>();
					identifiers.put(idProp, idValues);
					
					offer.setNodeID(nodeID);
					offer.setUrl(url);
					offer.setIdentifiers(identifiers);
				}
				
				offers.put(nodeID+url,offer);
			}
			
			reader.close();
		}
		
		writeOffers(offers.values());
	}

	private void writeOffers(Collection<OutputOffer> offers) throws IOException {
		BufferedWriter writer = new BufferedWriter (new FileWriter(transformedFile));
		for(OutputOffer o:offers) {
			writer.write(o.toJSONObject(false)+"\n");
		}
		writer.flush();
		writer.close();
	}
}
