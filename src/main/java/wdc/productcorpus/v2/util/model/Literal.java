package wdc.productcorpus.v2.util.model;
import wdc.productcorpus.v2.util.StringUtils;

public class Literal extends LiteralType {
	
	/**
	 * Creates a literal from its String
	 * basically removes the "" at the start and end
	 * 
	 * @param literal
	 */
	public Literal(String literal) {
		super(StringUtils.removeFirstLast(literal));
	}
	
	@Override
	public String toString() {
		return "\"" + this.value + "\"";
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
