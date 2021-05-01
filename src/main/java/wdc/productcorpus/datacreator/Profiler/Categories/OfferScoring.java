package wdc.productcorpus.datacreator.Profiler.Categories;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;
import wdc.productcorpus.datacreator.Profiler.SpecTables.SpecificationTable;

public class OfferScoring extends OutputOffer{

	private HashSet<String> words = new HashSet<String>();
	private HashMap<CategoryScoring, Double> categorysupport = new HashMap<CategoryScoring,Double>();
	private ArrayList<CategoryScoring> dominantCategories = new ArrayList<CategoryScoring>();
	ArrayList<CategoryScoring> categoryScoring;
	private boolean predicted = false;
	private String predictedLabel;
	private String correctLabel;
	
	private boolean title;
	private boolean description;
	private boolean specTables;
	
	
	public OfferScoring(String url, String nodeID, HashMap<String, HashSet<String>> identifiers,
			HashMap<String, HashSet<String>> descProperties, String cluster_id, ArrayList<CategoryScoring> categoryScoring, 
			HashMap<String, HashSet<String>> parentdescProperties, String propertyToParent, String parentNodeID, SpecificationTable tableSpecContent, boolean title, boolean description, boolean specTables) {
		
		super(url, nodeID, identifiers, descProperties, cluster_id,parentdescProperties,propertyToParent,parentNodeID, tableSpecContent);
		this.categoryScoring  = categoryScoring;
		this.title= title;
		this.description = description;
		this.specTables = specTables;
		
		this.setWords();
		this.setCategorysupport();
		this.setDominantCategory();
		
	}
	
	//for labelled offers
	public OfferScoring(String url, String nodeID, String label) {
		
		super(url, nodeID, null, null, null, null, null, null, null);
		
		CategoryScoring dominantCategory = new CategoryScoring();
		dominantCategory.setCategory_name(label);
		
		this.dominantCategories.add(dominantCategory);
		this.setCorrectLabel(label);
		
	}

	public OfferScoring(String url, String nodeID, HashMap<String, HashSet<String>> identifiers,
			HashMap<String, HashSet<String>> descProperties, String cluster_id,
			HashMap<String, HashSet<String>> parentdescProperties, String propertyToParent, String parentNodeID,
			SpecificationTable specTable, boolean title, boolean description, boolean specTables) {
		
		super(url, nodeID, identifiers, descProperties, cluster_id,parentdescProperties,propertyToParent,parentNodeID, specTable);
		this.title= title;
		this.description = description;
		this.specTables = specTables;
		
		this.setWords();
	}

	public HashSet<String> getWords() {
		return words;
	}
	
	public void setWords() {
		
		ArrayList<HashSet<String>> allDescriptiveValues = new ArrayList<HashSet<String>>();
		
		if (this.title) {
			if (null != this.getDescProperties().get("/name")) allDescriptiveValues.add(this.getDescProperties().get("/name"));
			if (null != this.getDescProperties().get("/title")) allDescriptiveValues.add(this.getDescProperties().get("/title"));
			if (null != this.getParentdescProperties().get("/name")) allDescriptiveValues.add(this.getParentdescProperties().get("/name"));
			if (null != this.getParentdescProperties().get("/title")) allDescriptiveValues.add(this.getParentdescProperties().get("/title"));			
		}
		
		if (this.description) {
			if (null != this.getDescProperties().get("/description")) allDescriptiveValues.add(this.getDescProperties().get("/description"));
			if (null != this.getDescProperties().get("/brand")) allDescriptiveValues.add(this.getDescProperties().get("/brand"));
			if (null != this.getParentdescProperties().get("/description")) allDescriptiveValues.add(this.getParentdescProperties().get("/description"));
			if (null != this.getParentdescProperties().get("/brand")) allDescriptiveValues.add(this.getParentdescProperties().get("/brand"));	
		}

		
		if (this.specTables && this.getSpecTable().getContent() != null ) {
			if (!this.getSpecTable().getContent().isEmpty()) 
				System.out.println("non empty");
			HashSet<String> tmp = new HashSet<>();
			tmp.add(this.getSpecTable().getContent());
			allDescriptiveValues.add(tmp);
		}
		
		for (HashSet<String> descriptions: allDescriptiveValues) {
			for (String description:descriptions){
				//apply some basic preprocessing - removal of punctuation, lowercase, remove language tags
				String [] tokens = description.replaceAll("(\"@.{2,4})","").replaceAll("\""," ").replaceAll("[-_]","").replaceAll("[^a-zA-Z]", " ").toLowerCase().split("\\s+");
				for (int i=0;i<tokens.length; i++){
					if (tokens[i].length()>0) this.words.add(tokens[i].trim());
				}
			}
		}
		 		
	}
	
	public HashMap<CategoryScoring, Double> getCategorysupport() {
		return categorysupport;
	}
	
	public void setCategorysupport() {
		
		//the words are fixed for all categories 
		Set<String> overlapOfWords = new HashSet<String>(this.categoryScoring.get(0).getWordScores().keySet());
		overlapOfWords.retainAll(this.words);
		
		for(CategoryScoring categ: this.categoryScoring) {
				
			double score=0.0;
			for (String w:overlapOfWords) {
				score += categ.getWordScores().get(w);
			}
			
			this.categorysupport.put(categ, score);
		}
	}
	
	public ArrayList<CategoryScoring> getDominantCategories() {
		return dominantCategories;
	}
	
	public void setDominantCategory() {
		
		HashMap<Double,ArrayList<CategoryScoring>> scores = new HashMap<Double,ArrayList<CategoryScoring>>();
		
		double maxScore= -1;
		for (Map.Entry<CategoryScoring, Double> catScore : this.categorysupport.entrySet()) {
			
			ArrayList<CategoryScoring> currentCategoriesOfThisScore = scores.get(catScore.getValue());
			if (null == currentCategoriesOfThisScore) currentCategoriesOfThisScore = new ArrayList<>();
			currentCategoriesOfThisScore.add(catScore.getKey());
			
			scores.put(catScore.getValue(), currentCategoriesOfThisScore);
			
			if (catScore.getValue()> maxScore) {
				maxScore = catScore.getValue();
			}
		}
		
		if (maxScore < 0.0001) this.dominantCategories = null;
		else this.dominantCategories = scores.get(maxScore);
	}
	
	public ArrayList<String> getDominantCategoriesNames() {
		
		ArrayList<String> dominantCategNames = new ArrayList<String>();
		
		if (this.dominantCategories != null) {
			for (CategoryScoring c:this.dominantCategories)
				dominantCategNames.add(c.getCategory_name());
		}
		else
			dominantCategNames.add("not found");
		
		return dominantCategNames;
			
	}

	public Boolean isPredicted() {
		return predicted;
	}

	public void setPredicted(boolean predicted) {
		this.predicted = predicted;
	}

	public String getPredictedLabel() {
		return predictedLabel;
	}

	public void setPredictedLabel(String predictedLabel) {
		this.predictedLabel = predictedLabel;
	}

	public String getCorrectLabel() {
		return correctLabel;
	}

	public void setCorrectLabel(String correctLabel) {
		this.correctLabel = correctLabel;
	}
}
