package wdc.productcorpus.datacreator.OutputFilesCreator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import org.json.JSONObject;

import de.dwslab.dwslib.util.io.InputUtil;

/**
 * @author Anna Primpeli
 * Import the data from the JSON file and creates the OutputOfers elements
 */
public class ClusterImporter {

	private File inputFile;
	
	public ClusterImporter(File offersFile) {
		this.inputFile = offersFile;
		
	}
	
	
	
	public ClusterImporter() {
		super();
	}

	public HashSet<OutputCluster> importClusters(boolean filterinSize3) {
		HashSet<OutputCluster> clusters = new HashSet<OutputCluster>();

		
		try {
			BufferedReader reader = InputUtil.getBufferedReader(inputFile);
			
			String line="";
			int clustersCounter = 0;
			while ((line=reader.readLine())!=null) {
				clustersCounter++;
				if (clustersCounter%1000000==0) System.out.println("Loaded "+clustersCounter+" clusters");
				JSONObject json = new JSONObject(line);
				
				OutputCluster cluster = jsonToCluster(json);
				
				if ((filterinSize3 && cluster.getSizeInOffers()>2) || !filterinSize3)
					clusters.add(cluster);
				
			}
		}
		catch(Exception e){
			System.out.println("[DataImporter]"+e.getMessage());
		}
		
		return clusters;
	}
	
	
	
	
	public OutputCluster jsonToCluster(JSONObject json) {
		
		Integer sizeInOffers = json.getInt("cluster_size_in_offers");
		String clusterID = json.get("id").toString();
		String category = json.get("category").toString();
		
		OutputCluster cluster = new OutputCluster();
		cluster.setId(clusterID);
		cluster.setCategory(category);
		cluster.setSizeInOffers(sizeInOffers);
		return cluster;
	} 
	

	public static void main(String args[]) throws IOException {
		ClusterImporter load = new ClusterImporter(new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\OfferInfo.json"));
		HashSet<OutputCluster> clusters = load.importClusters(true);
		System.out.println("Loaded "+clusters.size()+" clusters");
	}


}
