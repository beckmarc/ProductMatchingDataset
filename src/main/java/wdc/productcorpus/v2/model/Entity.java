package wdc.productcorpus.v2.model;

import java.util.ArrayList;
import java.util.HashSet;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ldif.entity.NodeTrait;

/**
 * Reprensents a product/offer entity in the corpus
 * 
 * @author beckm
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Entity {
	
	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	public int cluster_id;
	
	public String url;
	
	// Identifiers
	public String gtin12;
    public String gtin13;
    public String gtin14;
    public String gtin8;
    public String gtin;
    public String mpn;
    public String productID;
    public String sku;
    public String identifier;
    public String serialNumber;
    
    // Description Properties
    public String brand;
    public String name;
    public String title;
    public String description;
    public String price;
    public String priceCurrency;
    public String image;
    public String availability;
    public String manufacturer;
    
    // extra identifier for connecting child offers
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isVariation;
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean itemPage;
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isItem;
    
    // Metadata for the corpus
	public String fileId;
	
	@JsonAlias("nodeID")
	public String nodeId;
  	
 	@JsonIgnore
 	public boolean hasId;
 	@JsonIgnore
 	public boolean isOffer;
 	@JsonIgnore
 	public boolean isProduct;
}
