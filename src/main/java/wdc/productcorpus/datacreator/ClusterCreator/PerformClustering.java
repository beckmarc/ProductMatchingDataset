package wdc.productcorpus.datacreator.ClusterCreator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import wdc.productcorpus.datacreator.ClusterCreator.utils.Offer;

public abstract class PerformClustering {
	
	BufferedWriter writer;
	File output;
	
	public PerformClustering(File outputDirectory){
		output = outputDirectory;
	}
	

	protected abstract  ArrayList<ClusterElement> clusterOffers(ArrayList<Offer> offers);
	
	public void writeClusterInfo (ArrayList<ClusterElement> clusters) throws IOException {
		
		writer = new BufferedWriter (new FileWriter(output.toString()+"/clusters_info.txt",false));
		//write the clusters in order of their identifiers size
		Collections.sort(clusters, new Comparator<ClusterElement>() { 
			public int compare(ClusterElement o1, ClusterElement o2) {
				return (o2.ids.size() - o1.ids.size());

	        } });
		
		writer.write("Write key support information about "+clusters.size()+" cluster(s). \n");
		DecimalFormat df = new DecimalFormat("#.###");
		
		for (ClusterElement c:clusters) {
			c.calculateSupport();
			writer.write("CLUSTER : "+c.getIds()+" \n");
			for (Entry<Set<String>, Double> stats:c.supportRelativePerKeyCombination.entrySet())
				writer.write(stats.getKey()+" --> "+df.format(stats.getValue())+" , "+c.supportAbsPerKeyCombination.get(stats.getKey())+" \n");
		}
		
		checkOffersInMultipleClusters(clusters);
	}
	
	public void checkOffersInMultipleClusters (ArrayList<ClusterElement> clusters) throws IOException {
		
		writer.write("Check if any offer is allocated to more than one clusters. \n");

		//flip the vie to clusters per offer
		HashMap<String, ArrayList<String>> offersToClusters = new HashMap<String, ArrayList<String>>();
		
		for (ClusterElement c:clusters) {
			for (Offer o: c.getOffers()) {
				String key = o.getKey();
				ArrayList<String> currentClusters = offersToClusters.get(key);
				if (null == currentClusters) currentClusters = new ArrayList<String>();
				
				currentClusters.add(c.getIds().toString());
				offersToClusters.put(key, currentClusters);
			}
		}
		//now tell me how many offers are allocated in more than one cluster
		int counter = 0;
		for (Map.Entry<String, ArrayList<String>> o:offersToClusters.entrySet()) {
			if (o.getValue().size()>1) {
				counter ++;
				writer.write("Offer "+o.getKey()+" is allocated to "+o.getValue().size()+ " clusters:"+o.getValue().toString()+" \n");
			}
		}
		
		writer.write("In total "+counter+" offers were allocated to more than one clusters \n");
		
		System.out.println("In total "+counter+" offers were allocated to more than one clusters");
		writer.flush();
		writer.close();
	}
}
