package wdc.productcorpus.datacreator.Extractor;

public class SupervisedNode {

	private String nodeID;
	private String url;
	private String fileID;
	private String identifyingProperty;
	private String normalizedValue;
	private String text;
	
	public SupervisedNode(String nodeID, String url, String fileID, String identifyingProperty, String normalizedValue,
			String text) {
		super();
		this.nodeID = nodeID;
		this.url = url;
		this.fileID = fileID;
		this.identifyingProperty = identifyingProperty;
		this.normalizedValue = normalizedValue;
		this.text = text;
	}
	
	public SupervisedNode(String nodeID, String url, String identifyingProperty, String normalizedValue,
			String text) {
		super();
		this.nodeID = nodeID;
		this.url = url;
		this.identifyingProperty = identifyingProperty;
		this.normalizedValue = normalizedValue;
		this.text = text;
	}

	public String nodetoString(){
		return fileID+"\t"+nodeID+"\t"+url+"\t"+identifyingProperty+"\t"+normalizedValue+"\t"+"\""+text+"\"";
	}
	
	protected String getNodeID() {
		return nodeID;
	}

	protected void setNodeID(String nodeID) {
		this.nodeID = nodeID;
	}

	protected String getUrl() {
		return url;
	}

	protected void setUrl(String url) {
		this.url = url;
	}

	protected String getFileID() {
		return fileID;
	}

	protected void setFileID(String fileID) {
		this.fileID = fileID;
	}

	protected String getIdentifyingProperty() {
		return identifyingProperty;
	}

	protected void setIdentifyingProperty(String identifyingProperty) {
		this.identifyingProperty = identifyingProperty;
	}

	protected String getNormalizedValue() {
		return normalizedValue;
	}

	protected void setNormalizedValue(String normalizedValue) {
		this.normalizedValue = normalizedValue;
	}

	protected String getText() {
		return text;
	}

	protected void setText(String text) {
		this.text = text;
	}
	
	
	
	
	
}
