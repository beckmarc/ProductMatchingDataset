package wdc.productcorpus.datacreator.ClusterCreator;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import wdc.productcorpus.datacreator.ClusterCreator.utils.Offer;



public class StrictClustering extends PerformClustering{

	public StrictClustering(File outputDirectory) {
		super(outputDirectory);
		// TODO Auto-generated constructor stub
	}

	@Override
	public ArrayList <ClusterElement> clusterOffers(ArrayList<Offer> offers) {
				
		HashMap<String, ClusterElement> clusters = new HashMap<String, ClusterElement>();
		
		int counter =0;
		for (Offer o: offers) {
			counter++;
			if (counter%500000 ==0) System.out.println("Strict clustering parsed "+counter+" offers.");
			ArrayList<String> keys = new ArrayList<String>(o.getUniqueIdentifiers());
			
			//the offer might be deprecated
			if (keys.isEmpty()) continue;
			
			Collections.sort(keys);
			
			String searchInMapKey = keys.toString();
			
			ClusterElement c = clusters.get(searchInMapKey);
			if (null==c) {
				c = new ClusterElement();
				c.ids = new HashSet<String>(keys);
			}
			c.offers.add(o);
			clusters.put(searchInMapKey, c);

		}
		
		return new ArrayList<ClusterElement>(clusters.values());
	}
	
	
}
