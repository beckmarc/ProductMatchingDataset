package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.json.JSONException;
import org.json.JSONObject;

import de.dwslab.dwslib.util.io.InputUtil;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;

public class OutputOfferToCSV {

	static File jsonOffers = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\test");
	static File jsonLabels = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\cat_gs_Evaluation.txt");

	static File csvFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\cat_gs_fastText_test.csv");
	
	public static void main (String args[]) throws IOException {
		
		OutputOfferToCSV convert = new OutputOfferToCSV();
		convert.convert();
				
	}
	
	public void convert() throws IOException {
		
		HashMap<String,String> labels = loadLabels();
		
		DataImporter load = new DataImporter();
	
		BufferedReader reader = InputUtil.getBufferedReader(jsonOffers);
		
		String line="";

		BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile));
		while (reader.ready()) {
			line = reader.readLine();
			JSONObject json = new JSONObject(line);
			OutputOffer offer = load.jsonToOffer(json);
			String offerLabel = labels.get(offer.getUrl()+"_"+offer.getNodeID());
			String textOffer = setWords(offer);
			
			writer.write(offerLabel+"\t"+textOffer+"\n");
			
		}
		
		writer.flush();
		writer.close();
	}

	private HashMap<String,String> loadLabels() throws JSONException, IOException {
		
		BufferedReader reader = InputUtil.getBufferedReader(jsonLabels);
		
		String line="";

		HashMap<String,String> labels = new HashMap<String,String>();
		
		while (reader.ready()) {
			line = reader.readLine();
			JSONObject json = new JSONObject(line);
			
			String url = json.getString("url");
			String nodeID = json.getString("nodeID");
			String offerLabel = json.getString("categoryLabel");

			labels.put(url+"_"+nodeID, offerLabel);
			
			
		}
		
		return labels;
	}

	public String setWords(OutputOffer offer) {
		
		String concatText = "";
		
		ArrayList<HashSet<String>> allDescriptiveValues = new ArrayList<HashSet<String>>(offer.getDescProperties().values());
		allDescriptiveValues.addAll(offer.getParentdescProperties().values());
		
		HashSet<String> words = new HashSet<String>();
		
		for (HashSet<String> descriptions: allDescriptiveValues) {
			for (String description:descriptions){
				//apply some basic preprocessing - removal of punctuation, lowercase
				String [] tokens = description.replaceAll("\""," ").replaceAll("[^a-zA-Z]", " ").toLowerCase().split("\\s+");
				for (int i=0;i<tokens.length; i++){
					if (tokens[i].length()>0) words.add(tokens[i].trim());
				}
			}
		}
		
		for (String w:words)
			concatText+=w+" ";
		
		return concatText.trim();
		 		
	}
}
