package wdc.productcorpus.datacreator.ClusterCreator.utils;

import java.util.HashMap;
import java.util.HashSet;

import de.uni_mannheim.informatik.dws.winter.utils.graph.Edge;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Graph;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Node;

public class GraphElement {

	private HashMap<String , Node<ProductClass>> nodesOfGraph = new HashMap<String , Node<ProductClass>>();
	private HashSet<Edge<ProductClass, String>> edgesOfGraph = new HashSet<Edge<ProductClass, String>>();
	private HashSet<Component> components = new HashSet<Component>();
	private Graph<Node<ProductClass>, String> graphContent = new Graph<Node<ProductClass>, String>();
	
	public HashMap<String, Node<ProductClass>> getNodesOfGraph() {
		return nodesOfGraph;
	}
	public void setNodesOfGraph(HashMap<String, Node<ProductClass>> nodesOfGraph) {
		this.nodesOfGraph = nodesOfGraph;
	}
	public HashSet<Edge<ProductClass, String>> getEdgesOfGraph() {
		return edgesOfGraph;
	}
	public void setEdgesOfGraph(HashSet<Edge<ProductClass, String>> edgesOfGraph) {
		this.edgesOfGraph = edgesOfGraph;
	}
	public HashSet<Component> getComponents() {
		return components;
	}
	public void setComponents(HashSet<Component> components) {
		this.components = components;
	}
	public Graph<Node<ProductClass>, String> getGraphContent() {
		return graphContent;
	}
	public void setGraphContent(Graph<Node<ProductClass>, String> graphContent) {
		this.graphContent = graphContent;
	}

	public Integer getOffersCount() {
		Integer offerscount =0;
		for (Component c:components) {
			offerscount += c.getOffersCount();
		}
		return offerscount;
	}
}
