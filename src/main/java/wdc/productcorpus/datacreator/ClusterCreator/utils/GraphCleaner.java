package wdc.productcorpus.datacreator.ClusterCreator.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import de.uni_mannheim.informatik.dws.winter.utils.graph.Edge;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Graph;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Node;
import de.wbsg.loddesc.util.DomainUtils;
import wdc.productcorpus.datacreator.ClusterCreator.MergeClustersWithInduction;

public class GraphCleaner {

	private HashSet<String> nodesToBeFiltered = new HashSet<String>();
	private GraphElement graph = new GraphElement();
	private File outputDirectory;
	private int offerminsupport = 10;
	private int pldminsupport = 5;
	private double minCC= 0.2;
	private double maxDegreewithMinCC = 10;

	public GraphElement cleanGraph(GraphElement graph, boolean clean, boolean filterOffersCount, boolean filterPLDCount, File outputDirectory) throws IOException {
		
		this.graph = graph;
		this.outputDirectory = outputDirectory;
		
		if (clean) {				
			findClusterCoefficient();			
			System.out.println("Create clean graph without the "+nodesToBeFiltered.size()+" problematic nodes (high degree vertices with small clustering coefficient)");
			cleanGraph(this.graph.getNodesOfGraph(), this.graph.getEdgesOfGraph());
			nodesToBeFiltered.clear();
			System.out.println("[GraphCleaner] After removing the high cc nodes the resulting graph has "+this.graph.getNodesOfGraph().size()+" nodes and "+this.graph.getEdgesOfGraph().size()+" edges.");
			System.out.println("[GraphCleaner] After removing the high cc nodes the resulting graph has "+this.graph.getGraphContent().getDataNodes().size()+" nodes (should be the same as above)");

		}
		
		this.graph.setComponents(clusterOffers());
		
		System.out.println("[GraphCleaner] Created Components after CC filtering:"+this.graph.getComponents().size());
		
		if (filterOffersCount) {
			System.out.println("Filter out the components that have less than 10 offers");
			findWeakOfferCountSupport();
		}
		
		if (filterPLDCount) {
			System.out.println("Filter out the components whose offers derive from less than 5 plds");
			findWeakPLDSupport();
		}
		
		if (nodesToBeFiltered.size()>0) {
			cleanGraph(this.graph.getNodesOfGraph(), this.graph.getEdgesOfGraph());
		}
		return this.graph;
		
	}
	
	private void findWeakOfferCountSupport(){
		
		HashSet<Component> filterComponents = new HashSet<Component>();
		for (Component c: this.graph.getComponents()) {
			if (c.getOffersCount()<offerminsupport) {
				filterComponents.add(c);
				for (Node<ProductClass> n :c.getNodes())
					nodesToBeFiltered.add(n.getData().toString());
			}
		}
		
		this.graph.getComponents().removeAll(filterComponents);
	}
	
	private void findWeakPLDSupport(){
		
		System.out.println("[GraphCleaner] Components BEFORE filtering of weak pld support: "+this.graph.getComponents().size());

		HashSet<Component> filterComponents = new HashSet<Component>();
		
		for (Component c: this.graph.getComponents()) {
			HashSet<String> plds = new HashSet<String>();
			HashSet<String> nodesInfo = new HashSet<String>();
			for (Node<ProductClass> n: c.getNodes()) {
				
				nodesInfo.add(n.getData().toString());
				for (Offer o:n.getData().getOffers()) {
					String pld = DomainUtils.getDomain(o.getKey().split("\\t")[1]);
					if (null != pld) plds.add(pld);
					
				}
			}
			
			if (plds.size()<pldminsupport) {
				filterComponents.add(c);
				nodesToBeFiltered.addAll(nodesInfo);
			}
		}
		
		this.graph.getComponents().removeAll(filterComponents);
		System.out.println("[GraphCleaner] Components AFTER filtering of weak pld support: "+this.graph.getComponents().size());
	}
	
	private void findClusterCoefficient() throws IOException {
		
		System.out.println("Print clustering coefficient for possibly problematic vertices");
		System.out.println("Remember a cc of -1 describes an independent cluster while a cc of 0.0 describes a cluster whose neighbours are not connected");
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/nodesWithSmallCC.txt",false));

		for (Node<Node<ProductClass>> node:this.graph.getGraphContent().getGraphNodes()) {
			double cc = node.getData().getData().getClusterCoefficient();
			//print out the problematic nodes : remember a cc of -1 describes an independent cluster while a cc of 0.0 describes a cluster whose neighbours are not connected
			//print the ones with a high in degree (for the smaller ones we need to check the support)
			if (cc>=0.0 && cc<minCC && node.getData().getData().getDegree()>=maxDegreewithMinCC) {
				writer.write("Node: "+node.getData().toString()+" has cc :"+cc+" and an degree of "+node.getData().getData().getAdjacentNodes().size()+"\n");
				this.nodesToBeFiltered.add(node.getData().toString());
			}
		}
		
		writer.flush();
		writer.close();
		
	}

	private void cleanNodes() {
		
		System.out.println("Clean "+this.nodesToBeFiltered.size()+" nodes from  "+this.graph.getComponents().size()+" components of the graph");
		HashSet<Component> compToRemove = new HashSet<Component>();
		for (Component c: this.graph.getComponents()) {
			HashSet<Node<ProductClass>> tobeRemovedFromComp = new HashSet<Node<ProductClass>>();
			for (Node<ProductClass> n: c.getNodes()) {
				if (this.nodesToBeFiltered.contains(n.getData().toString())) tobeRemovedFromComp.add(n);			
			}
			c.getNodes().removeAll(tobeRemovedFromComp);
			if (c.getNodes().isEmpty()) compToRemove.add(c);

		}
		this.graph.getComponents().removeAll(compToRemove);
	}
	
	private HashSet<Component> clusterOffers() throws IOException {

		MergeClustersWithInduction cluster = new MergeClustersWithInduction();
		HashSet<Component> components = cluster.clusterWithInduction(this.graph.getGraphContent());
		
		return components;
	}

	private void cleanGraph(HashMap<String, Node<ProductClass>> nodesOfGraph, HashSet<Edge<ProductClass, String>> edgesOfGraph) {
		
		try {
			//filter from the graph those nodes that are contained in "weak support components" and nodes with small cc
			this.graph.setGraphContent(new Graph<Node<ProductClass>, String>());
			HashMap<String, Node<ProductClass>> nodesOfGraph_updated = new HashMap<String, Node<ProductClass>>();
			HashSet<Edge<ProductClass, String>> edgesOfGraph_updated = new HashSet<Edge<ProductClass, String>>();
			
			for (Edge<ProductClass, String> e: edgesOfGraph) {
				boolean include = true;
				for (Node<ProductClass> n: e.getNodes()){
					if (nodesToBeFiltered.contains(n.toString())) include = false;

				}
				if (include) {
					ArrayList<Node<ProductClass>> nodes = new ArrayList<Node<ProductClass>>(e.getNodes());
					if (nodes.size()!=2) {
						System.out.println("Something goes very wrong. One edge should be between two nodes. STOP.");
						System.exit(0);
					}
					
					Node<ProductClass> nodeS_clean = removeUncleanNeighbours(nodes.get(0));
					Node<ProductClass> nodeT_clean = removeUncleanNeighbours(nodes.get(1));

					this.graph.getGraphContent().addEdge(nodeS_clean, nodeT_clean, "sameAs", 1.0);
					
					nodesOfGraph_updated.put(nodeS_clean.getData().getId(), nodeS_clean);
					nodesOfGraph_updated.put(nodeT_clean.getData().getId(), nodeT_clean);
					edgesOfGraph_updated.add(new Edge<ProductClass, String>(nodeS_clean, nodeT_clean, "sameAs", 1.0));
				}
			}
			
			for (Map.Entry<String, Node<ProductClass>> n: nodesOfGraph.entrySet()) {
				if (!nodesToBeFiltered.contains(n.getKey())){
					this.graph.getGraphContent().addNode(removeUncleanNeighbours(n.getValue()));
					nodesOfGraph_updated.put(n.getKey(), removeUncleanNeighbours(n.getValue()));
				}
			}
			
			
			System.out.println("Write pajek network format for the whole CLEAN graph in "+outputDirectory+"/completepajekFileCLEAN.net");
			this.graph.getGraphContent().writePajekFormat(new File(outputDirectory+"/completepajekFileCLEAN.net"));
			
			// we do not need anymore this info
			this.graph.setNodesOfGraph(nodesOfGraph_updated); 
			this.graph.setEdgesOfGraph(edgesOfGraph_updated);
		}
		catch(Exception e) {
			System.out.println(e.getStackTrace().toString());
			System.out.println(e.getMessage());
		}

	}

	private Node<ProductClass> removeUncleanNeighbours(Node<ProductClass> node) {
		
		HashMap<String, Node<ProductClass>> badNeighbours = new HashMap<String,Node<ProductClass>>();
		//remove the unclean neighbours
		for (Map.Entry<String,Node<ProductClass>> neighbour: node.getData().getAdjacentNodes().entrySet()){
			if (nodesToBeFiltered.contains(neighbour.getValue().getData().getId())){								
				badNeighbours.put(neighbour.getKey(), neighbour.getValue());		
			}
		}
		
		for (Map.Entry<String,Node<ProductClass>> bad: badNeighbours.entrySet()) {
			node.getData().getAdjacentNodes().remove(bad.getKey(),bad.getValue());
		}
		return node;
		
	}


}
