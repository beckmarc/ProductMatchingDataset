package wdc.productcorpus.datacreator.OutputFilesCreator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import wdc.productcorpus.util.InputUtil;

/**
 * @author Anna Primpeli
 * Count words from titles and descriptions
 */
public class WordCounter {

	File  file= new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\offers_sample.txt");
	File output = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\word_count.txt");

	HashMap<String,Integer> wordCounter = new HashMap();
	
	public static void main (String args[]) throws Exception {
		WordCounter count = new WordCounter();
		if (args.length != 0) {
			count.file = new File (args[0]);
			count.output = new File (args[1]);
		}
		count.readDescription(count.file);
		count.printResults(count.output);
		
		
	}
	
	public void readDescription(File input) throws IOException {
		BufferedReader br = InputUtil.getBufferedReader(file);
		
		String line;
		while ((line = br.readLine()) != null) {
			JSONObject json = new JSONObject(line);
			JSONArray desc = (JSONArray) json.get("schema.org_description");
			HashSet<String> distinctTokens = new HashSet();
			String text = "";
			
			for (int i= 0; i<desc.length();i++) {
				JSONObject d = (JSONObject) desc.get(i);
				if (d.has("/name")) text = text.concat(" "+d.get("/name"));
				if (d.has("/title")) text = text.concat(" "+d.get("/title"));
				if (d.has("/brand")) text = text.concat(" "+d.get("/brand"));
				
			} 
			
			String normalizedtext = normalizeText(text);
			String [] alltokens= normalizedtext.trim().split("\\s");
			for (int j=0;j<alltokens.length;j++)
				distinctTokens.add(alltokens[j]);
			integrateCounter(distinctTokens);
		}
		br.close();

	}
	
	
	
	private void integrateCounter(HashSet<String> distinctTokens) {
		for (String t:distinctTokens) {
			Integer currentCount = wordCounter.get(t);
			if (null == currentCount) currentCount =0;
			currentCount++;
			wordCounter.put(t, currentCount);
		}
		
	}

	private void printResults(File output) throws IOException  {
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(output));
		for (Map.Entry<String, Integer> wc: wordCounter.entrySet())
			writer.write(wc.getKey()+"\t"+wc.getValue()+"\n");
		
		writer.flush();
		writer.close();
		
	}
	private String normalizeText(String text) {
		String normalizedText = text.replaceAll("\"@.*\\s?"," ").replaceAll("\\r\\n+|\\r+|\\n+", " ").replaceAll("[^A-Za-z0-9\\s]+"," ").replaceAll("[\\s\\t]+", " ").toLowerCase();
		return normalizedText;
	}
	
}
