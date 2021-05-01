package wdc.productcorpus.datacreator.ClusterCreator.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class Offer {

	private String key;
	private HashMap <String, ArrayList<String>> propValue = new HashMap<String, ArrayList<String>>();
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public HashMap<String, ArrayList<String>> getPropValue() {
		return propValue;
	}
	public void setPropValue(HashMap<String, ArrayList<String>> propValue) {
		this.propValue = propValue;
	}
	
	public HashSet<String> getUniqueIdentifiers() {
		HashSet<String> uniqueIdentifiers = new HashSet<String>();
		
		for (Map.Entry<String, ArrayList<String>> v:propValue.entrySet()) {
			for (String value:v.getValue())
				uniqueIdentifiers.add(value);
		}
		
		return uniqueIdentifiers;
	}
	
	@Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Offer offer = (Offer) o;
 
        return key != null ? key.equals(offer.key) : offer.key == null;
    }
    @Override
    public int hashCode() {
        int result = (key != null ? key.hashCode() : 0);
        return result;
    }
	
}
