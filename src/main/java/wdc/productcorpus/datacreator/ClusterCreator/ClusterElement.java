package wdc.productcorpus.datacreator.ClusterCreator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import wdc.productcorpus.datacreator.ClusterCreator.utils.Offer;

public class ClusterElement {

	HashSet<String> ids = new HashSet<String>();
	HashSet<Offer> offers = new HashSet<Offer>();
	
	HashMap<Set<String>, Double> supportRelativePerKeyCombination = new HashMap<Set<String>, Double>();
	HashMap<Set<String>, Integer> supportAbsPerKeyCombination = new HashMap<Set<String>, Integer>();
	
	ClusterElement parentCluster = null;
	ArrayList<ClusterElement> childClusters = new ArrayList<ClusterElement>();
	
	
	public void calculateSupport() {
		
		Set<Set<String>> idCombos = new HashSet<Set<String>>();
		ClusterElementsUtils utils = new ClusterElementsUtils();
		
		idCombos = (Set<Set<String>>) Sets.powerSet(this.ids);
		
		//update absolute support
		for (Offer o:this.offers) {
			HashSet<String> offerIDs = o.getUniqueIdentifiers();
			
			for (Set<String> keyCombo: idCombos){
				if (utils.isSameIDSet(offerIDs, keyCombo)) {
					Integer currentCounter = this.supportAbsPerKeyCombination.get(keyCombo);
					if (null == currentCounter) currentCounter=0;
					currentCounter ++;
					this.supportAbsPerKeyCombination.put(keyCombo, currentCounter);
					break;
				}
			}
		}
		
		//now calculate the relative support
		for (Map.Entry<Set<String>, Integer> keystats: this.supportAbsPerKeyCombination.entrySet())
			this.supportRelativePerKeyCombination.put(keystats.getKey(), (double) keystats.getValue()/(double) offers.size());
	}

	protected HashSet<String> getIds() {
		return ids;
	}

	protected void setIds(HashSet<String> ids) {
		this.ids = ids;
	}

	protected HashSet<Offer> getOffers() {
		return offers;
	}

	protected void setOffers(HashSet<Offer> offers) {
		this.offers = offers;
	}

	@Override
    public boolean equals(Object c) {
        if (this == c) return true;
        if (c == null || getClass() != c.getClass()) return false;
        ClusterElement cluster = (ClusterElement) c;

        ClusterElementsUtils utils = new ClusterElementsUtils();
        
        return ids != null ? utils.isSameIDSet(ids, cluster.ids) : cluster.ids == null;
    }
    @Override
    public int hashCode() {
    	List<String> idsList = new ArrayList<String>(ids);
    	Collections.sort(idsList);
        int result = (idsList.toString() != null ? idsList.toString().hashCode() : 0);
        return result;
    }

}
