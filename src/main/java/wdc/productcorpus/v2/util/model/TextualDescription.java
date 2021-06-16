package wdc.productcorpus.v2.util.model;

/**
 * 
 * @author beckm
 *
 */
public class TextualDescription {
	public String brand;
	public String name;
	public String title;
	public String description;
	
	public TextualDescription(String brand, String name, String title, String description) {
		super();
		this.brand = brand;
		this.name = name;
		this.title = title;
		this.description = description;
	}

	public TextualDescription() {
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public String toString() {
		return "brand: " + brand + " name: " + name + " title: " + title + " description: " + description;
	}
	
}
