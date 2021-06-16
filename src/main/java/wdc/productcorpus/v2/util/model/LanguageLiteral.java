package wdc.productcorpus.v2.util.model;

public class LanguageLiteral extends LiteralType {

	private String language;
	
	public LanguageLiteral(String value, String language) {
		super(value);
		this.language = language;
	}
	
	/**
	 * Creates a new Language Literal from a String 
	 * 
	 * @param literal the literal value e.g. "12345fds"@de
	 */
	public LanguageLiteral(String literal) {
		super();
		String[] objectArray = literal.trim().substring(1).split("\"@");
		value = objectArray[0].trim();
		this.language = objectArray[1].trim();
	}
	
	@Override
	public String toString() {
		return "\"" + this.value.trim() + "\"@" + this.language.trim();
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}
	
	/**
	 * Checks whether a string can be language literal or not
	 * @param literal
	 * @return
	 */
	public static boolean isType(String literal) {
		return literal.matches("\".*\"@.*");
	}
}
