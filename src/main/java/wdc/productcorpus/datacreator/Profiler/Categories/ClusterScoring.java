package wdc.productcorpus.datacreator.Profiler.Categories;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import wdc.productcorpus.datacreator.OutputFilesCreator.OutputCluster;

public class ClusterScoring extends OutputCluster {

	private ArrayList<OfferScoring> offersScoringInfo = new ArrayList<OfferScoring>();
	private CategoryScoring dominantCategory = new CategoryScoring();
	private double categoryDensity;
	

	public ArrayList<OfferScoring> getOffersScoringInfo() {
		return offersScoringInfo;
	}
	public void setOffersScoringInfo(ArrayList<OfferScoring> offersScoringInfo) {
		this.offersScoringInfo = offersScoringInfo;
	}
	
	public CategoryScoring getDominantCategory() {
		return dominantCategory;
	}
	
	public double getCategoryDensity() {
		return categoryDensity;
	}
	
	//get dominant category by voting
	public void addCategoryInfo() {
		
		HashMap<CategoryScoring, Integer> categories = new HashMap<CategoryScoring, Integer>();
		
		for (OfferScoring offer: offersScoringInfo) {
			if (offer.getDominantCategories()==null || offer.getDominantCategories().isEmpty()) continue;

			for (CategoryScoring c:offer.getDominantCategories()) {
				Integer currentValue = categories.get(c);
				if (null == currentValue) currentValue = 0;
				categories.put(c, ++currentValue);
			}
		}
		
		if (categories.isEmpty()) {			
			this.dominantCategory=null;
			this.categoryDensity =0.0;
		}
		else {
			this.dominantCategory = Collections.max(categories.entrySet(), (entry1, entry2) -> entry1.getValue() - entry2.getValue()).getKey();
			Integer totalVotes = 0;
			for (Integer v :categories.values())
				totalVotes += v;
			this.categoryDensity = ((double) categories.get(this.dominantCategory))/ ((double) totalVotes);
		}
		
	}
	
	
	
	
	
}
