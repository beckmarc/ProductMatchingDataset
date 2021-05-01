package wdc.productcorpus.datacreator.ClusterCreator;


import java.util.Set;

public class ClusterElementsUtils {

	public boolean isSameIDSet(Set<String> ids1, Set<String> ids2) {
		if (ids1.containsAll(ids2) && ids2.containsAll(ids1)) return true;
		else return false;
	}
	
	public boolean isSuperClusterOf(ClusterElement cluster1, ClusterElement cluster2) {
		if (cluster2.ids.size()>cluster1.ids.size()) return false;
		else {
			return cluster1.ids.containsAll(cluster2.ids);
		}
	}
	
	public ClusterElement mergeClusters(ClusterElement c1, ClusterElement c2) {
		ClusterElement merged = new ClusterElement();
		merged.ids.addAll(c1.ids);
		merged.ids.addAll(c2.ids);
		
		merged.offers.addAll(c1.offers);
		merged.offers.addAll(c2.offers);
						
		return merged;
	}

}
