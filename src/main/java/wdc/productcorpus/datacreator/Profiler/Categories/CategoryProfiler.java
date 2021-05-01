package wdc.productcorpus.datacreator.Profiler.Categories;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

import de.dwslab.dwslib.util.io.InputUtil;

public class CategoryProfiler {

	HashMap<String,Category> statsCategories =  new HashMap<String, Category>();
	
	public static void main (String args[]) throws IOException {
		CategoryProfiler profile = new CategoryProfiler();
		HashMap<String,Integer> clusters_meta = profile.getClusterSizes(new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\idclusters.json.gz"));
		System.out.println("Got data for "+clusters_meta.size()+" clusters");
		profile.getStatsReport(new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Categorization\\categories_offers_en_clusters.csv.gz"), clusters_meta);
	}
	
	public HashMap<String,Integer> getClusterSizes(File clustersmeta) throws IOException {
		HashMap<String,Integer> clusterSizes = new HashMap<String,Integer>();
		BufferedReader reader = InputUtil.getBufferedReader(clustersmeta);
		
		String line="";
		while ((line=reader.readLine())!=null) {
			JSONObject json = new JSONObject(line);
			String cluster_id = String.valueOf(json.getInt("id"));
			Integer cluster_size = json.getInt("cluster_size_in_offers");
			clusterSizes.put(cluster_id, cluster_size);
			
		}
		
		return clusterSizes;
	}
	public void getStatsReport(File categoriesmeta, HashMap<String,Integer> clustersizes) throws IOException{
		BufferedReader reader = InputUtil.getBufferedReader(categoriesmeta);
		String line="";
		boolean skipHeader = true;
		int nometadatafound = 0;
		while ((line=reader.readLine())!=null) {
			if (skipHeader) {
				skipHeader=false;
				continue;
			}
			String [] lineparts = line.split(",");
			String category = lineparts[0];
			String cluster_id = lineparts[1];
			
			Integer size_cluster = clustersizes.get(cluster_id);
			if (null==size_cluster) {
				nometadatafound +=1;
				System.out.println("No metadata found for cluster with id "+cluster_id);
				continue;
			}
			
			Category cat = statsCategories.get(category);
			if (null == cat){
				cat = new Category();
				cat.setId(category);
			}
			cat.setClustersCount((cat.getClustersCount()+1));
			cat.setOfferscount((cat.getOfferscount()+size_cluster));
			if (size_cluster==3 || size_cluster==4){
				cat.setClusters_3_4_count((cat.getClusters_3_4_count()+1));
			}
			else if (size_cluster>=5 && size_cluster<=10){
				cat.setClusters_5_10_count((cat.getClusters_5_10_count()+1));
			}
			else if (size_cluster>=11 && size_cluster<=20){
				cat.setClusters_11_20_count((cat.getClusters_11_20_count()+1));
			}
			else if (size_cluster>20) {
				cat.setClusters_large_count((cat.getClusters_large_count()+1));
			}
			
			statsCategories.put(category, cat);
		
		}
		System.out.println("No metadata found for "+nometadatafound+" clusters");
		System.out.println("Category;# Clusters;# Offers;[3-4];[5-10];[11-20];[>20]");
		for (Map.Entry<String, Category> c: statsCategories.entrySet()) {
			System.out.println(c.getKey()+";"+c.getValue().getClustersCount()+";"+c.getValue().getOfferscount()+";"+c.getValue().getClusters_3_4_count()+";"+
					c.getValue().getClusters_5_10_count()
					+";"+c.getValue().getClusters_11_20_count()+";"+c.getValue().getClusters_large_count());
		}
	}
}
