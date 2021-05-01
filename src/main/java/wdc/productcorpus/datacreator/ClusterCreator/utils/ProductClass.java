package wdc.productcorpus.datacreator.ClusterCreator.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import de.uni_mannheim.informatik.dws.winter.utils.graph.Node;

public class ProductClass {

	private String id;
	private ArrayList<Offer> offers;
	
	private HashMap<String, Node<ProductClass>> adjacentNodes = new HashMap<String, Node<ProductClass>>();
	
	public ProductClass(String id, ArrayList<Offer> offers) {
		this.id = id;
		this.offers = offers;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public ArrayList<Offer> getOffers() {
		return offers;
	}
	public void setOffers(ArrayList<Offer> offers) {
		this.offers = offers;
	}
	
	@Override
	public String toString(){
		return id;
	}
	public double getClusterCoefficient() {
		
		//if it has no or only one neighbours return 1
		if (this.getDegree() == null || this.getDegree() == 0 || this.getDegree() == 1 ) return -1.0;
		
		int actualConnections = 0;
		int maxConnections = this.getDegree() * (this.getDegree()-1)/2;	
			
		HashSet<String> visitedNeighbours = new HashSet<String>();

		for(Map.Entry<String, Node<ProductClass>> neighbor: this.adjacentNodes.entrySet()) {
			for(Map.Entry<String, Node<ProductClass>> neighbor_: this.adjacentNodes.entrySet()) {
				if (!neighbor.getKey().equals(neighbor_.getKey()) 
						&& !visitedNeighbours.contains(neighbor_.getKey())
						&& neighbor_.getValue().getData().adjacentNodes.containsKey(neighbor.getKey()))
					actualConnections++;
			}
			visitedNeighbours.add(neighbor.getKey());
		}
		
		return ((double) actualConnections/ (double) maxConnections);
	}
	
	public Integer getDegree() {
		return adjacentNodes.size();
	}
	

	public HashMap<String, Node<ProductClass>> getAdjacentNodes() {
		return adjacentNodes;
	}
	public void setAdjacentNodes(HashMap<String, Node<ProductClass>> adjacentNodes) {
		this.adjacentNodes = adjacentNodes;
	}
}
