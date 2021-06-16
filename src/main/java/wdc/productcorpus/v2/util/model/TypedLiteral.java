package wdc.productcorpus.v2.util.model;

import wdc.productcorpus.v2.util.StringUtils;

/**
 * Typed Literal
 * e.g. "2020-03-16T18:58:30+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>
 * 
 * @author beckm
 *
 */
public class TypedLiteral extends LiteralType {
	
	private String type;
	
	public TypedLiteral(String value, String type) {
		this.value = value;
		this.type = type;
	}
	
	/**
	 * Creates a new Typed Literal from a String.
	 * splits this: "2020-03-16T18:58:30+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>
	 * 
	 * @param literal the literal value e.g. "12345fds"@de
	 */
	public TypedLiteral(String literal) {
		super();
		String[] objectArray = StringUtils.removeFirstLast(literal).split("\"\\^\\^<");
		this.value = objectArray[0].trim();
		this.type = objectArray[1].trim();
	}
	
	@Override
	public String toString() {
		return "\"" + this.value.trim() + "\"^^" + this.type.trim();
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
	/**
	 * Checks whether a string can be typed literal or not
	 * @param literal
	 * @return
	 */
	public static boolean isType(String literal) {
		return literal.matches("\".*\"\\^\\^<.*>");
	}
}
