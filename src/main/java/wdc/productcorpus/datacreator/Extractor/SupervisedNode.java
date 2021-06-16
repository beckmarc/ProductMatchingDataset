package wdc.productcorpus.datacreator.Extractor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import wdc.productcorpus.v2.util.PrintUtils;
import wdc.productcorpus.v2.util.StringUtils;
import wdc.productcorpus.v2.util.model.TextualDescription;

public class SupervisedNode {

	private String nodeID;
	private String url;
	private String fileID;
	private String identifyingProperty;
	private String normalizedValue;	
	private String name;
	private String title;
	private String description;
	private String brand;
	
	public SupervisedNode(String nodeID, String url, String fileID, String identifyingProperty, String normalizedValue,
			String name, String title, String brand, String description) {
		super();
		this.nodeID = nodeID;
		this.url = url;
		this.fileID = fileID;
		this.identifyingProperty = identifyingProperty;
		this.normalizedValue = normalizedValue;
		this.name = normalizeText(StringUtils.removeFirstLast(name));
		this.title = normalizeText(StringUtils.removeFirstLast(title));
		this.description = normalizeText(StringUtils.removeFirstLast(description));
		this.brand = normalizeText(StringUtils.removeFirstLast(brand));
	}

	public SupervisedNode(String nodeID, String url, String identifyingProperty, String normalizedValue, String name,
			String title,  String brand, String description) {
		super();
		this.nodeID = nodeID;
		this.url = url;
		this.identifyingProperty = identifyingProperty;
		this.normalizedValue = normalizedValue;
		this.name = normalizeText(StringUtils.removeFirstLast(name));
		this.title = normalizeText(StringUtils.removeFirstLast(title));
		this.description = normalizeText(StringUtils.removeFirstLast(description));
		this.brand = normalizeText(StringUtils.removeFirstLast(brand));
	}

	/**
	 * Creates a node by parsing the given line with the help of a Regex
	 * 
	 * @param line the tab seperated line containing all the necessary information
	 */
	public SupervisedNode(String line) {
		super();
		String lineParts[] = line.split("\\t");
		this.fileID = lineParts[0];
		this.nodeID = lineParts[1];
		this.url = lineParts[2];
		this.identifyingProperty = lineParts[3];
		this.normalizedValue = lineParts[4];
		this.name = StringUtils.removeFirstLast(lineParts[5]);
		this.title = StringUtils.removeFirstLast(lineParts[6]);
		this.description = StringUtils.removeFirstLast(lineParts[7]);
		this.brand = StringUtils.removeFirstLast(lineParts[8]);
	}
	
	public String normalizeText(String text) {
		String normalizedText = text.replaceAll("(\\s{2,})|(\\\\n)|(\\\\r)|(\\\\t)", ""); 
		normalizedText = normalizedText.replaceAll("\u00A0", "").trim();
		return normalizedText;
	}

	/**
	 * Adds quotation marks to the textual fields
	 * @return
	 */
	public String nodetoString(){
		return fileID+"\t"+nodeID+"\t"+url+"\t"+identifyingProperty+"\t"+normalizedValue+"\t"+
				"\""+name+"\""+"\t"+
				"\""+description+"\""+"\t"+
				"\""+brand+"\""+"\t"+
				"\""+title+"\"";
	}
	
	/**
	 * Gets the concatenation of all textual description properties
	 * @return
	 */
	public String getText() {
		return (name.trim() + " " + description.trim() + " " + brand.trim() + " " + title.trim()).trim();
	}
	
	public String getNodeID() {
		return nodeID;
	}

	public void setNodeID(String nodeID) {
		this.nodeID = nodeID;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getFileID() {
		return fileID;
	}

	public void setFileID(String fileID) {
		this.fileID = fileID;
	}

	public String getIdentifyingProperty() {
		return identifyingProperty;
	}

	public void setIdentifyingProperty(String identifyingProperty) {
		this.identifyingProperty = identifyingProperty;
	}

	public String getNormalizedValue() {
		return normalizedValue;
	}

	public void setNormalizedValue(String normalizedValue) {
		this.normalizedValue = normalizedValue;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}
	
	
	
	
}
