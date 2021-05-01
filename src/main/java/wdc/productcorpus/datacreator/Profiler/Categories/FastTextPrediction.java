package wdc.productcorpus.datacreator.Profiler.Categories;

import java.util.HashMap;

public class FastTextPrediction {

	private String domLabel;
	private Double domConfidence;
	
	private HashMap<String,Double> labels_confidences;

	public String getDomLabel() {
		return domLabel;
	}

	public void setDomLabel(String domLabel) {
		this.domLabel = domLabel;
	}

	public Double getDomConfidence() {
		return domConfidence;
	}

	public void setDomConfidence(Double domConfidence) {
		this.domConfidence = domConfidence;
	}

	public HashMap<String, Double> getLabels_confidences() {
		return labels_confidences;
	}

	public void setLabels_confidences(HashMap<String, Double> labels_confidences) {
		this.labels_confidences = labels_confidences;
	}
}
