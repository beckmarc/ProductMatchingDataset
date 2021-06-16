package wdc.productcorpus.v2.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import wdc.productcorpus.v2.util.PrintUtils;

/**
 * Static class that contains methods for the Entity class
 * Divided because Entity serves as POJO and is used for (de)serialization
 * @author beckm
 *
 */
public class EntityStatic {
	
	public static void setIdentifyingProperty(Entity entity, String property, String value) {
		switch(property) {
			case "/gtin12>":
				entity.gtin12 = value;
				break;
			case "/gtin13>":
				entity.gtin13 = value;
				break;
			case "/gtin14>":
				entity.gtin14 = value;
				break;
			case "/gtin8>":
				entity.gtin8 = value;
				break;
			case "/mpn>":
				entity.mpn = value;
				break;
			case "/productID>":
				entity.productID = value;
				break;
			case "/sku>":
				entity.sku = value;
				break;
			case "/identifier>":
				entity.identifier = value;
				break;	
			case "/gtin>":
				entity.gtin = value;
				break;	
			case "/serialNumber>":
				entity.serialNumber = value;
				break;	
		}
		
	}
	
	public static void setTextualProperty(Entity entity, String property, String value) {
		switch(property) {
			case "/title>":
				entity.title = value;
				break;
			case "/name>":
				entity.name = value;
				break;
			case "/description>":
				entity.description = value;
				break;
			case "/brand>":
				entity.brand = value;
				break;
			case "/price>":
				entity.price = value;
				break;
			case "/priceCurrency>":
				entity.priceCurrency = value;
				break;
			case "/image>":
				entity.image = value;
				break;
			case "/availability>":
				entity.availability = value;
				break;
			case "/manufacturer>":
				entity.manufacturer = value;
				break;
		}			
	}
	
	static ObjectMapper objectMapper = new ObjectMapper();
	
	/**
	 * Tries to parse an Entity. 
	 * @param line
	 * @return The entity or null if parsing failed.
	 */
	public static Entity parseEntity(String line) {
		try {
			return objectMapper.readValue(line, Entity.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static ArrayList<String> getIdentifiers(Entity e) {
		ArrayList<String> ids = new ArrayList<String>();
		if(e.gtin12 != null)
			ids.add(e.gtin12);
		if(e.gtin13 != null)
			ids.add(e.gtin13);
		if(e.gtin14 != null)
			ids.add(e.gtin14);
		if(e.gtin8 != null)
			ids.add(e.gtin8);
		if(e.mpn != null)
			ids.add(e.mpn);
		if(e.productID != null)
			ids.add(e.productID);
		if(e.sku != null)
			ids.add(e.sku);
		if(e.identifier != null)
			ids.add(e.identifier);
		if(e.gtin != null)
			ids.add(e.gtin);
		if(e.serialNumber != null)
			ids.add(e.serialNumber);
		return ids; 
	}
	
	public static boolean hasIdentifier(Entity e) {
		return getIdentifiers(e).isEmpty() ? false : true;
	}
	
	public static ArrayList<String> getDescriptions(Entity e) {
		ArrayList<String> ids = new ArrayList<String>();
		if(e.brand != null)
			ids.add(e.brand);
		if(e.name != null)
			ids.add(e.name);
		if(e.title != null)
			ids.add(e.title);
		if(e.description != null)
			ids.add(e.description);
		if(e.priceCurrency != null)
			ids.add(e.priceCurrency);
		if(e.price != null)
			ids.add(e.price);
		if(e.image != null)
			ids.add(e.image);
		if(e.availability != null)
			ids.add(e.availability);
		if(e.manufacturer != null)
			ids.add(e.manufacturer);
		return ids;
	}
	
	public static HashMap<String,String> getMappedIdentifiers(Entity e) {
		HashMap<String,String> ids = new HashMap<String,String>();
		if(e.gtin12 != null)
			ids.put("gtin12", e.gtin12);
		if(e.gtin13 != null)
			ids.put("gtin13", e.gtin13);
		if(e.gtin14 != null)
			ids.put("gtin14", e.gtin14);
		if(e.gtin8 != null)
			ids.put("gtin8", e.gtin8);
		if(e.mpn != null)
			ids.put("mpn", e.mpn);
		if(e.productID != null)
			ids.put("productID", e.productID);
		if(e.sku != null)
			ids.put("sku", e.sku);
		if(e.identifier != null)
			ids.put("identifier", e.identifier);
		if(e.gtin != null)
			ids.put("gtin", e.gtin);
		if(e.serialNumber != null)
			ids.put("serialNumber", e.serialNumber);
		return ids;
	}
	
	/**
	 * Gets all Attributes that are present on an entity mapped to their values
	 * @param e
	 * @return
	 */
	public static HashMap<String, String> getMappedAttributes(Entity e) {
		HashMap<String,String> atts = new HashMap<String,String>();
		// identifiers
		if(e.gtin12 != null)
			atts.put("gtin12", e.gtin12);
		if(e.gtin13 != null)
			atts.put("gtin13", e.gtin13);
		if(e.gtin14 != null)
			atts.put("gtin14", e.gtin14);
		if(e.gtin8 != null)
			atts.put("gtin8", e.gtin8);
		if(e.mpn != null)
			atts.put("mpn", e.mpn);
		if(e.productID != null)
			atts.put("productID", e.productID);
		if(e.sku != null)
			atts.put("sku", e.sku);
		if(e.identifier != null)
			atts.put("identifier", e.identifier);
		if(e.gtin != null)
			atts.put("gtin", e.gtin);
		// descriptive
		if(e.brand != null)
			atts.put("brand", e.brand);
		if(e.name != null)
			atts.put("name", e.name);
		if(e.title != null)
			atts.put("title", e.title);
		if(e.description != null)
			atts.put("description", e.description);
		if(e.priceCurrency != null)
			atts.put("priceCurrency", e.priceCurrency);
		if(e.price != null)
			atts.put("price", e.price);
		if(e.image != null)
			atts.put("image", e.image);
		if(e.availability != null)
			atts.put("availability", e.availability);
		if(e.manufacturer != null)
			atts.put("manufacturer", e.manufacturer);
		// other
		if(e.isVariation)
			atts.put("variations", "true");
		return atts;
	}
	
	/**
	 * Gets all Attributes that are present on an entity
	 * @param e
	 * @return
	 */
	public static HashSet<String> getAttributes(Entity e) {
		HashSet<String> atts = new HashSet<String>();
		// identifiers
		if(e.gtin12 != null)
			atts.add("gtin12");
		if(e.gtin13 != null)
			atts.add("gtin13");
		if(e.gtin14 != null)
			atts.add("gtin14");
		if(e.gtin8 != null)
			atts.add("gtin8");
		if(e.mpn != null)
			atts.add("mpn");
		if(e.productID != null)
			atts.add("productID");
		if(e.sku != null)
			atts.add("sku");
		if(e.identifier != null)
			atts.add("identifier");
		if(e.gtin != null)
			atts.add("gtin");
		// descriptive
		if(e.brand != null)
			atts.add("brand");
		if(e.name != null)
			atts.add("name");
		if(e.title != null)
			atts.add("title");
		if(e.description != null)
			atts.add("description");
		if(e.priceCurrency != null)
			atts.add("priceCurrency");
		if(e.price != null)
			atts.add("price");
		if(e.image != null)
			atts.add("image");
		if(e.availability != null)
			atts.add("availability");
		if(e.manufacturer != null)
			atts.add("manufacturer");
		// other
		if(e.isVariation)
			atts.add("variations");
		return atts;
	}
	
	
	
	public static String toJson(Entity e) {
		try {
			return objectMapper.writeValueAsString(e);
		} catch (JsonProcessingException e1) {
			e1.printStackTrace();
		}
		return null;
	}
	
	public static String toJson(ArrayList<Entity> es) {
		String result = "";
		try {
			for(Entity e : es) {
				result += objectMapper.writeValueAsString(e) + "\n";
			}
		} catch (JsonProcessingException e1) {
			e1.printStackTrace();
		}
		return result;
	}
	
	public static String toJson(HashSet<Entity> es) {
		String result = "";
		try {
			for(Entity e : es) {
				result += objectMapper.writeValueAsString(e) + "\n";
			}
		} catch (JsonProcessingException e1) {
			e1.printStackTrace();
		}
		return result;
	}
	
	public static boolean equals(Entity e1, Entity e2) {
		return e1.nodeId.equals(e2.nodeId) && e1.url.equals(e2.url);
	}
	
	public static boolean isContained(Entity e1, ArrayList<Entity> list) {
		for(Entity e : list) {
			if(equals(e, e1)) {
				return true;
			}
		}
		return false;
	}
	
	public static int getPropertiesLength(Entity e) {
		int result = 0;
		for(String s : getIdentifiers(e)) {
			result += s.length();
		}
		for(String s : getDescriptions(e)) {
			result += s.length();
		}
		return result;
	}
	
	/**
	 * Merges the Entity e into the parent entity. This means that every property which is unset on the parent will be set<br>
	 * if it is present on the e entity
	 * @param parent
	 * @param e
	 * @return the parent entity
	 */
	public static Entity mergeInto(Entity parent, Entity e) {
		// textual props
		parent.name = parent.name != null ? parent.name : e.name;
		parent.description = parent.description != null ? parent.description : e.description;
		parent.brand = parent.brand != null ? parent.brand : e.brand;
		parent.title = parent.title != null ? parent.title : e.title;
		parent.price = parent.price != null ? parent.price : e.price;
		parent.priceCurrency = parent.priceCurrency != null ? parent.priceCurrency : e.priceCurrency;
		parent.availability = parent.availability != null ? parent.availability : e.availability;
		parent.manufacturer = parent.manufacturer != null ? parent.manufacturer : e.manufacturer;
		parent.image = parent.image != null ? parent.image : e.image;
		// id props
		parent.gtin8 = parent.gtin8 != null ? parent.gtin8 : e.gtin8;
		parent.gtin12 = parent.gtin12 != null ? parent.gtin12 : e.gtin12;
		parent.gtin13 = parent.gtin13 != null ? parent.gtin13 : e.gtin13;
		parent.gtin14 = parent.gtin14 != null ? parent.gtin14 : e.gtin14;
		parent.gtin = parent.gtin != null ? parent.gtin : e.gtin;
		parent.serialNumber = parent.serialNumber != null ? parent.serialNumber : e.serialNumber;
		parent.mpn = parent.mpn != null ? parent.mpn : e.mpn;
		parent.productID = parent.productID != null ? parent.productID : e.productID;
		parent.sku = parent.sku != null ? parent.sku : e.sku;
		parent.identifier = parent.identifier != null ? parent.identifier : e.identifier;
		 // child entity will not be selected
		e = null;
		return parent;
	}
	
	/**
	 * Merges product and offer. Offer is set to hasId=false to filter it out
	 * @param parent
	 * @param e
	 */
	public static void mergeProductAndOffer(Entity parent, Entity e) {
		// textual props
		parent.name = parent.name != null ? parent.name : e.name;
		parent.description = parent.description != null ? parent.description : e.description;
		parent.brand = parent.brand != null ? parent.brand : e.brand;
		parent.title = parent.title != null ? parent.title : e.title;
		parent.price = parent.price != null ? parent.price : e.price;
		parent.priceCurrency = parent.priceCurrency != null ? parent.priceCurrency : e.priceCurrency;
		parent.availability = parent.availability != null ? parent.availability : e.availability;
		parent.manufacturer = parent.manufacturer != null ? parent.manufacturer : e.manufacturer;
		parent.image = parent.image != null ? parent.image : e.image;
		// id props
		parent.gtin8 = parent.gtin8 != null ? parent.gtin8 : e.gtin8;
		parent.gtin12 = parent.gtin12 != null ? parent.gtin12 : e.gtin12;
		parent.gtin13 = parent.gtin13 != null ? parent.gtin13 : e.gtin13;
		parent.gtin14 = parent.gtin14 != null ? parent.gtin14 : e.gtin14;
		parent.gtin = parent.gtin != null ? parent.gtin : e.gtin;
		parent.serialNumber = parent.serialNumber != null ? parent.serialNumber : e.serialNumber;
		parent.mpn = parent.mpn != null ? parent.mpn : e.mpn;
		parent.productID = parent.productID != null ? parent.productID : e.productID;
		parent.sku = parent.sku != null ? parent.sku : e.sku;
		parent.identifier = parent.identifier != null ? parent.identifier : e.identifier;
		 // child entity will not be selected
		e.hasId = false;
		if(!parent.hasId && hasIdentifier(parent)) { 
			// if the parent property had no id before, check if it got one from its child offer
			parent.hasId = true;
		}
	}
	
	public static void mergeEntities(Entity parent, Entity e) {
		e.name = e.name != null ? e.name : parent.name;
		parent.name = parent.name != null ? parent.name : e.name;
		e.description = e.description != null ? e.description : parent.description;
		parent.description = parent.description != null ? parent.description : e.description;
		e.brand = e.brand != null ? e.brand : parent.brand;
		parent.brand = parent.brand != null ? parent.brand : e.brand;
		e.title = e.title != null ? e.title : parent.title;
		parent.title = parent.title != null ? parent.title : e.title;
		e.manufacturer = e.manufacturer != null ? e.manufacturer : parent.manufacturer;
		parent.manufacturer = parent.manufacturer != null ? parent.manufacturer : e.manufacturer;
		e.price = e.price != null ? e.price : parent.price;
		parent.price = parent.price != null ? parent.price : e.price;
		e.priceCurrency = e.priceCurrency != null ? e.priceCurrency : parent.priceCurrency;
		parent.priceCurrency = parent.priceCurrency != null ? parent.priceCurrency : e.priceCurrency;
	}

	public static void removeIdentifierByValue(String idValue, Entity e) {
		if(e.gtin12 != null && e.gtin12.equals(idValue))
			e.gtin12 = null;
		if(e.gtin13 != null && e.gtin13.equals(idValue))
			e.gtin13 = null;
		if(e.gtin14 != null && e.gtin14.equals(idValue))
			e.gtin14 = null;
		if(e.gtin8 != null && e.gtin8.equals(idValue))
			e.gtin8 = null;
		if(e.gtin != null && e.gtin.equals(idValue))
			e.gtin = null;
		if(e.sku != null && e.sku.equals(idValue))
			e.sku = null;
		if(e.mpn != null && e.mpn.equals(idValue))
			e.mpn = null;
		if(e.serialNumber != null && e.serialNumber.equals(idValue))
			e.serialNumber = null;
		if(e.productID != null && e.productID.equals(idValue))
			e.productID = null;
		if(e.identifier != null && e.identifier.equals(idValue))
			e.identifier = null;	
	}
	
	
	
	
	
	
	

}
