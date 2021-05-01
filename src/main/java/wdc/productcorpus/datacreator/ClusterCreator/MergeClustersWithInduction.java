package wdc.productcorpus.datacreator.ClusterCreator;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import de.uni_mannheim.informatik.dws.winter.utils.graph.Graph;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Node;
import wdc.productcorpus.datacreator.ClusterCreator.utils.Component;
import wdc.productcorpus.datacreator.ClusterCreator.utils.ProductClass;

public class MergeClustersWithInduction {

	/**
	 * @param graph
	 * Create the connected components of the graph
	 * @throws IOException 
	 */
//	public HashSet<Component> clusterWithInduction(Graph<Node<ProductClass>, String> graph) throws IOException {
//		
//		System.out.println("[MergeClustersWithInduction] Data Nodes: "+graph.getDataNodes().size());
//		System.out.println("[MergeClustersWithInduction] Graph Nodes: "+graph.getGraphNodes().size());
//
//		
//		HashMap<Integer, Component> components = new HashMap<Integer, Component>();
//		HashMap<String, Integer> nodeToCompIndex = new HashMap<String, Integer>();
//		
//		int componentIDs = 1;
//		for (Node<ProductClass> node: graph.getDataNodes()) {
//			
//			HashSet<Node<ProductClass>> tmpcomponent = new HashSet<Node<ProductClass>>();
//			
//			Integer component = nodeToCompIndex.get(node.getData().getId());
//			tmpcomponent.add(node);
//			if(null == component) {
//				for (Node<ProductClass> neighbour: node.getData().getAdjacentNodes().values()){
//					if (null != nodeToCompIndex.get(neighbour.getData().getId()))
//						component = nodeToCompIndex.get(neighbour.getData().getId());
//					tmpcomponent.add(neighbour);
//				}
//			}
//			
//			if (null == component) { 
//				component = componentIDs;
//				componentIDs++;
//				components.put(component, new Component(component));
//			}
//			components.get(component).getNodes().addAll(tmpcomponent);
//			
//			//add to the index
//			for (Node<ProductClass> compEntry: tmpcomponent)
//				nodeToCompIndex.put(compEntry.getData().getId(), component);
//		}
//		
//		System.out.println("[MergeClustersWithInduction] Distinct nodes: "+nodeToCompIndex.size());
//		System.out.println("[MergeClustersWithInduction] Created "+components.size()+" components.");
//		return new HashSet<Component> (components.values());
//		
//	}
	
	
	public HashSet<Component> clusterWithInduction(Graph<Node<ProductClass>, String> graph) throws IOException {
		
		System.out.println("[MergeClustersWithInduction] Data Nodes: "+graph.getDataNodes().size());
		System.out.println("[MergeClustersWithInduction] Graph Nodes: "+graph.getGraphNodes().size());
		
		
		HashMap<Integer, Component> components = new HashMap<Integer, Component>();
		HashMap<String, Integer> nodeToCompIndex = new HashMap<String, Integer>();
		
		int componentIDs = 1;
		for (Node<ProductClass> node: graph.getDataNodes()) {
			
			HashSet<Node<ProductClass>> familyNodes = new HashSet<Node<ProductClass>>();

			HashSet<Integer> matchingComponents = new HashSet<Integer>();
			Integer exComp = nodeToCompIndex.get(node.getData().getId());
			familyNodes.add(node);
			
			if (null!= exComp) matchingComponents.add(exComp);
			
			for (Node<ProductClass> neighbour : node.getData().getAdjacentNodes().values()) {
				familyNodes.add(neighbour);
				exComp = nodeToCompIndex.get(neighbour.getData().getId());
				if (null!= exComp) matchingComponents.add(exComp);
			}
			
			int component = -1;
			//if there were no existing components matching add a new component
			if (matchingComponents.size()==0) {
				component = componentIDs;
				componentIDs++;
				components.put(component, new Component(component));
				components.get(component).getNodes().addAll(familyNodes);
				
			}
			//if there is one component matching add the current node to this component
			if (matchingComponents.size() == 1) {
				component = new ArrayList<Integer>(matchingComponents).get(0);
				components.get(component).getNodes().addAll(familyNodes);
			}
	
			//if there are more than one component matching create a new component by merging the matching ones
			if (matchingComponents.size()>1) {
				
				HashSet<Node<ProductClass>> mergedComponent = new HashSet<Node<ProductClass>>();
				for (Integer c:matchingComponents) {
					mergedComponent.addAll(components.get(c).getNodes());
					components.remove(c);
				}
				familyNodes.addAll(mergedComponent);
				
				component = componentIDs;
				componentIDs++;
				components.put(component, new Component(component));
				components.get(component).getNodes().addAll(familyNodes);
			}
			
			if (component == -1) System.out.println("[MergeClustersWithInduction] Component was not set correctly. Please check." );
			//update index
			for (Node<ProductClass> n:familyNodes) {
				nodeToCompIndex.put(n.getData().getId(), component);
			}
		}
		
		System.out.println("[MergeClustersWithInduction] Distinct nodes: "+nodeToCompIndex.size());
		System.out.println("[MergeClustersWithInduction] Created "+components.size()+" components.");
		return new HashSet<Component> (components.values());
		
	}

	

	
}
