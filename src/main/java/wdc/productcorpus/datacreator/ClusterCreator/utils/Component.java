package wdc.productcorpus.datacreator.ClusterCreator.utils;

import java.util.HashSet;

import de.uni_mannheim.informatik.dws.winter.utils.graph.Node;

public class Component {

	private HashSet<Node<ProductClass>> nodes = new HashSet<Node<ProductClass>>();
	private Integer componentID ;
	
	public Component(Integer componentID) {
		this.componentID =componentID;
	}
	public HashSet<Node<ProductClass>> getNodes() {
		return nodes;
	}
	public void setNodes(HashSet<Node<ProductClass>> nodes) {
		this.nodes = nodes;
	}
	public Integer getOffersCount() {
		
		return getOffers().size();
	}
	
	public HashSet<Offer> getOffers() {
		
		HashSet<Offer> offers = new HashSet<Offer>();
		for (Node<ProductClass> n:nodes)
			offers.addAll(new HashSet<Offer>(n.getData().getOffers()));
		
		return offers;
	}
	
	public Integer getSize() {
		return nodes.size();
	}
	
	public Integer getComponentID() {
		return componentID;
	}
	public void setComponentID(Integer componentID) {
		this.componentID = componentID;
	}
	
	
	@Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Component component = (Component) o;
 
        return componentID != null ? componentID.equals(component.componentID) : component.componentID == null;
    }
    @Override
    public int hashCode() {
        int result = (componentID != null ? componentID.hashCode() : 0);
        return result;
    }
}
