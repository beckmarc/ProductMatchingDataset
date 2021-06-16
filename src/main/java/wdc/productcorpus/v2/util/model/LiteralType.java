package wdc.productcorpus.v2.util.model;

public abstract class LiteralType {

	public abstract String getValue();
	public abstract void setValue(String value);
	public abstract String toString();
	
	public String value;
	
	public LiteralType(String value) {
		this.value = value;
	}
	
	public LiteralType() {}
}
