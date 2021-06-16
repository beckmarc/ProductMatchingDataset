package wdc.productcorpus.datacreator.OutputFilesCreator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import wdc.productcorpus.datacreator.Profiler.SpecTables.SpecificationTable;

public class OutputOffer {

	private String url;
	private String nodeID;
	private HashMap<String, HashSet<String>> identifiers = new HashMap<String, HashSet<String>>() ;
	private HashMap<String, HashSet<String>> descProperties = new HashMap<String, HashSet<String>>();
	private String cluster_id;
	
	private HashMap<String, HashSet<String>> parentdescProperties = new HashMap<String, HashSet<String>>();
	private String propertyToParent;
	private String parentNodeID;
	private HashMap<String, HashSet<String>> nonprocessedIdentifiers = new HashMap<String, HashSet<String>>();
	
	
	//private String specTableContent;
	
	public OutputOffer() {}
	
	
	
	public OutputOffer(String url, String nodeID) {
		super();
		this.url = url;
		this.nodeID = nodeID;
	}




	
	public OutputOffer(String url, String nodeID, HashMap<String, HashSet<String>> identifiers,
			HashMap<String, HashSet<String>> descProperties, String cluster_id,
			HashMap<String, HashSet<String>> parentdescProperties, String propertyToParent, String parentNodeID, SpecificationTable specTable) {
		super();
		this.url = url;
		this.nodeID = nodeID;
		this.identifiers = identifiers;
		this.descProperties = descProperties;
		this.cluster_id = cluster_id;
		this.parentdescProperties = parentdescProperties;
		this.propertyToParent = propertyToParent;
		this.parentNodeID = parentNodeID;
		this.specTable = specTable;
	}


	public OutputOffer(OutputOffer outputOffer) {
		this.url = outputOffer.url;
		this.nodeID = outputOffer.nodeID;
		this.identifiers = outputOffer.identifiers;
		this.descProperties = outputOffer.descProperties;
		this.cluster_id = outputOffer.cluster_id;
		this.parentdescProperties = outputOffer.parentdescProperties;
		this.propertyToParent = outputOffer.propertyToParent;
		this.parentNodeID = outputOffer.parentNodeID;
		this.specTable = outputOffer.specTable;
		this.nonprocessedIdentifiers=outputOffer.nonprocessedIdentifiers;
	}



	public String getDescriptivePropertiesAsOneString() {
		
		String concattext = "";
		for(HashSet<String> values : this.descProperties.values()) {
			concattext = concattext + (String.join("", values));
		}
		
		return concattext;

		
	}
	
	public String getIdentifierPropertiesAsOneString() {
		
		String concattext = "";
		for(HashSet<String> values : this.identifiers.values()) {
			concattext = concattext + (String.join("", values));
		}
		
		return concattext;

		
	}

	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getNodeID() {
		return nodeID;
	}
	public void setNodeID(String nodeID) {
		this.nodeID = nodeID;
	}
	public HashMap<String, HashSet<String>> getIdentifiers() {
		return identifiers;
	}
	public void setIdentifiers(HashMap<String, HashSet<String>> identifiers) {
		this.identifiers = identifiers;
	}
	public HashMap<String, HashSet<String>> getDescProperties() {
		return descProperties;
	}
	public void setDescProperties(HashMap<String, HashSet<String>> descProperties) {
		this.descProperties = descProperties;
	}
	public String getCluster_id() {
		return cluster_id;
	}
	public void setCluster_id(String cluster_id) {
		this.cluster_id = cluster_id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((nodeID == null) ? 0 : nodeID.hashCode());
		result = prime * result + ((url == null) ? 0 : url.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OutputOffer other = (OutputOffer) obj;
		if (nodeID == null) {
			if (other.nodeID != null)
				return false;
		} else if (!nodeID.equals(other.nodeID))
			return false;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		return true;
	}



	public HashMap<String, HashSet<String>> getParentdescProperties() {
		return parentdescProperties;
	}



	public void setParentdescProperties(HashMap<String, HashSet<String>> parentdescProperties) {
		this.parentdescProperties = parentdescProperties;
	}



	public String getPropertyToParent() {
		return propertyToParent;
	}



	public void setPropertyToParent(String propertyToParent) {
		this.propertyToParent = propertyToParent;
	}



	public String getParentNodeID() {
		return parentNodeID;
	}



	public void setParentNodeID(String parentNodeID) {
		this.parentNodeID = parentNodeID;
	}

	
	public JsonObject toJSONObject(boolean addParentInfo) {
		
		JsonObject offer_item = new JsonObject();
		offer_item.addProperty("url", this.url);
		offer_item.addProperty("nodeID", this.nodeID);
		offer_item.addProperty("cluster_id", this.cluster_id);
		
		JsonArray id_array = new JsonArray();
		for (Map.Entry<String, HashSet<String>> ids : this.identifiers.entrySet()){
			JsonObject id = new JsonObject();
			id.addProperty(ids.getKey(), ids.getValue().toString());
			id_array.add(id);
		}
		offer_item.add("identifiers", id_array);
		
		JsonArray id_array_non_norm = new JsonArray();
		for (Map.Entry<String, HashSet<String>> non_norm_ids : this.nonprocessedIdentifiers.entrySet()){
			JsonObject id = new JsonObject();
			id.addProperty(non_norm_ids.getKey(), non_norm_ids.getValue().toString());
			id_array_non_norm.add(id);
		}
		offer_item.add("non_normalized_identifiers", id_array_non_norm);
		
		JsonArray desc_array = new JsonArray();
		for (Map.Entry<String, HashSet<String>> desc : this.descProperties.entrySet()){
			JsonObject d = new JsonObject();
			d.addProperty(desc.getKey(), desc.getValue().toString());
			desc_array.add(d);
		}
		offer_item.add("schema.org_properties", desc_array);
		
		if (addParentInfo) {
			offer_item.addProperty("parent_NodeID", this.parentNodeID);
			offer_item.addProperty("relationToParent", this.getPropertyToParent());
			
			JsonArray parent_desc_array = new JsonArray();
			for (Map.Entry<String, HashSet<String>> desc : this.parentdescProperties.entrySet()){
				JsonObject d = new JsonObject();
				d.addProperty(desc.getKey(), desc.getValue().toString());
				parent_desc_array.add(d);
			}
			
			offer_item.add("parent_schema.org_properties", parent_desc_array);

		}
		
		return offer_item;
	}
	
	public OutputOffer jsonToOffer(JSONObject json) {
		
		String url = json.getString("url");
		String nodeID = json.getString("nodeID");
		String clusterID = json.getString("cluster_id");
		
		JSONArray desc = (JSONArray) json.get("schema.org_properties");
		HashMap<String, HashSet<String>> desc_values = getValuesFromJSONArray(desc, false);
		JSONArray identifiers = (JSONArray) json.get("identifiers");
		HashMap<String, HashSet<String>> identifier_values = getValuesFromJSONArray(identifiers, true);
		HashMap<String, HashSet<String>> non_norm_identifier_values = new HashMap<String, HashSet<String>>();
		if (json.has("non_normalized_identifiers")) {
			JSONArray nonormidentifiers = (JSONArray) json.get("non_normalized_identifiers");			
			non_norm_identifier_values = getValuesFromJSONArray(nonormidentifiers, true);
		}
		
		
		
		JSONArray parentdescProperties = (JSONArray) json.get("parent_schema.org_properties");
		HashMap<String, HashSet<String>> parentdescProperties_values = getValuesFromJSONArray(parentdescProperties, true);
		
		String propertyToParent = json.get("relationToParent").toString();
		String parentNodeID = json.get("parent_NodeID").toString();
		
		OutputOffer offer = new OutputOffer();
		offer.setUrl(url);
		offer.setNodeID(nodeID);
		offer.setCluster_id(clusterID);
		offer.setDescProperties(desc_values);
		offer.setIdentifiers(identifier_values);
		offer.setParentdescProperties(parentdescProperties_values);
		offer.setParentNodeID(parentNodeID);
		offer.setPropertyToParent(propertyToParent);
		offer.setNonprocessedIdentifiers(non_norm_identifier_values);
		
		return offer;
	} 
	
	/**
	 * @param array
	 * @param isIdentifiers
	 * @return
	 */
	public HashMap<String, HashSet<String>> getValuesFromJSONArray(JSONArray array, boolean isIdentifiers) {
		
		HashMap<String, HashSet<String>> valuesOfArray = new HashMap();
		
		
		for (int i= 0; i< array.length(); i++) {
			JSONObject o = array.getJSONObject(i);
			Set<String> keys = o.keySet();
			for (String k:keys) {
				HashSet<String> arrayValuesAsSet = new HashSet<String>();
				if (isIdentifiers){
					String [] arrayValues = o.get(k).toString().replaceAll("\\[", "").replaceAll("\\]", "").split(",");				
					for (int j =0; j<arrayValues.length;j++)
						arrayValuesAsSet.add(arrayValues[j]);
				}
				else arrayValuesAsSet.add(o.get(k).toString().replaceAll("\\[", "").replaceAll("\\]", ""));
				
				valuesOfArray.put(k, arrayValuesAsSet);
			}
		}
		
		return valuesOfArray;
	}


	public SpecificationTable getSpecTable() {
		return specTable;
	}



	public void setSpecTable(SpecificationTable specTable) {
		this.specTable = specTable;
	}



	public HashMap<String, HashSet<String>> getNonprocessedIdentifiers() {
		return nonprocessedIdentifiers;
	}



	public void setNonprocessedIdentifiers(HashMap<String, HashSet<String>> nonprocessedIdentifiers) {
		this.nonprocessedIdentifiers = nonprocessedIdentifiers;
	}



	
}
