package wdc.productcorpus.datacreator.OutputFilesCreator;

import java.util.HashSet;

public class OutputCluster {

	private String id;
	private HashSet<String> offerIDs = new HashSet<String>();
	private HashSet<String> identifiers = new HashSet<String>();
	private String category;
	private int sizeInOffers;
	
	public OutputCluster(){}
	
	public OutputCluster(String cluster_id) {
		this.id = cluster_id;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public HashSet<String> getOfferIDs() {
		return offerIDs;
	}
	public void setOfferIDs(HashSet<String> offerIDs) {
		this.offerIDs = offerIDs;
	}
	public HashSet<String> getIdentifiers() {
		return identifiers;
	}
	public void setIdentifiers(HashSet<String> identifiers) {
		this.identifiers = identifiers;
	}
	
	public int getClusterSize(){
		return offerIDs.size();
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public int getSizeInOffers() {
		return sizeInOffers;
	}
	public void setSizeInOffers(int sizeInOffers) {
		this.sizeInOffers = sizeInOffers;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
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
		OutputCluster other = (OutputCluster) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}
	
	
}
