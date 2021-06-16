package wdc.productcorpus.datacreator.OutputFilesCreator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

import wdc.productcorpus.datacreator.Extractor.EnhancedIdentifierExtractor;
import wdc.productcorpus.util.InputUtil;

public class SearchInClusterFiles {
	
	File clusterFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\idclusters.json.gz");
	File  keywordsFile= new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\phones_id.txt");

	ArrayList<String> normalizedIDValues = new ArrayList<String>();
	HashMap<String,String> productstoIDs = new HashMap<String,String>();
	HashMap<String,String> productsToClusters = new HashMap<String,String>();
	
	public static void main (String args[]) throws Exception {
		SearchInClusterFiles search = new SearchInClusterFiles();
		search.readKeywords(search.keywordsFile);
		search.search(search.clusterFile);
		search.printResults();
		
	}
	
	public void readKeywords(File input) throws IOException {
		BufferedReader br = InputUtil.getBufferedReader(input);
		String line;
		while ((line = br.readLine()) != null) {
			String rawValue = line.split("\\|\\|")[0].trim();
			String normalizedValue = EnhancedIdentifierExtractor.normalizeValue(rawValue);
			normalizedIDValues.add(normalizedValue);
			productstoIDs.put(rawValue, line.split("\\|\\|")[2]);
		}

	}
	
	protected void search(File clusterFile) throws Exception {
		
		BufferedReader br = InputUtil.getBufferedReader(clusterFile);
		
		String line;
		while ((line = br.readLine()) != null) {
			JSONObject json = new JSONObject(line);
			String id_values [] = json.getString("id_values").replaceAll("\\[","").replaceAll("\\]","").split(",");
			for (int i=0; i<id_values.length;i++) {
				if (normalizedIDValues.contains(id_values[i].trim())) {
					String cluster_id = json.getString("id");
					String relatedProduct = this.productstoIDs.get(id_values[i].trim());
					productsToClusters.put(relatedProduct, cluster_id);
					
				}
			}
			
			
		}
		br.close();
	}
	
	
	private synchronized void printResults()  {
		
		System.out.println("Mapped "+productsToClusters.size()+" products to clusters");
		for (Map.Entry<String, String> e:this.productsToClusters.entrySet())
			System.out.println(e.getKey()+" in cluster "+e.getValue());
		
	}
	
	
}
