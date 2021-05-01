package wdc.productcorpus.datacreator.Profiler.Categories;

import java.util.HashMap;

public class CategoryScoring {

	private String category_name;
	private HashMap<String,Double> wordScores = new HashMap<String,Double>();
	
	public String getCategory_name() {
		return category_name;
	}
	public void setCategory_name(String category_name) {
		this.category_name = category_name;
	}
	public HashMap<String, Double> getWordScores() {
		return wordScores;
	}
	public void setWordScores(HashMap<String, Double> wordScores) {
		this.wordScores = wordScores;
	}
}
