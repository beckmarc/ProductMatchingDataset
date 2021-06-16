package wdc.productcorpus.datacreator.ClusterCreator.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import de.uni_mannheim.informatik.dws.winter.utils.graph.Edge;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Node;
import wdc.productcorpus.v2.util.PrintUtils;

public class GraphGenerator {

	public GraphElement createBasicGraph(HashSet<Offer> offers, HashSet<String> uniqueIdentifiers, File outputDirectory) throws IOException {
			
			System.out.println("Start buiding graph");
			GraphElement graph = new GraphElement();
					
			ArrayList<String> identifiers = new ArrayList<String>(uniqueIdentifiers);
			
			HashMap<String , Node<ProductClass>> nodesOfGraph = new HashMap<String,Node<ProductClass>>();
			
			HashSet<Edge<ProductClass, String>> edgesOfGraph = new HashSet<Edge<ProductClass, String>>();
			
			
			System.out.println("Create nodes");
			System.out.println(identifiers.size());
	
			//create a vertex for every unique identifier
			for (int i=0; i<identifiers.size();i++) {
				ProductClass prclass = new ProductClass(identifiers.get(i), new ArrayList<Offer>());
				
				Node<ProductClass> node = new Node<ProductClass>(prclass, i);
				nodesOfGraph.put(identifiers.get(i), node);			
			}
			
			System.out.println("Create edges");
			//now look at the offer and create the edges of the graph
			for (Offer offer:offers) {
				for (String id:offer.getUniqueIdentifiers()) {
					//add the offer info in the corresponding vertex of the graph
					
//					if(nodesOfGraph.get(id) != null) {
						nodesOfGraph.get(id).getData().getOffers().add(offer);
//					} else {
//						PrintUtils.p(id);
//						PrintUtils.p(nodesOfGraph.get(id));
//						PrintUtils.p(offer.toString());
//						PrintUtils.p(offer.getPropValue());
//					}
					
			
					for (String id_:offer.getUniqueIdentifiers()) {
						//create an edge between all the identifier values (as vertices) of the same offer
						if (!id.equals(id_)){
							Edge<ProductClass, String> edge = new Edge<ProductClass, String>(nodesOfGraph.get(id), nodesOfGraph.get(id_), "sameAs", 1.0);
							edgesOfGraph.add(edge);
	
							//add the adjacent nodes information
							nodesOfGraph.get(id).getData().getAdjacentNodes().put(id_, nodesOfGraph.get(id_));
							nodesOfGraph.get(id_).getData().getAdjacentNodes().put(id, nodesOfGraph.get(id));
							
							//add the edge in the graph
							graph.getGraphContent().addEdge(nodesOfGraph.get(id), nodesOfGraph.get(id_), "sameAs", 1.0);
							
						}
					}
					
					//add the complete node in the offer graph
					graph.getGraphContent().addNode(nodesOfGraph.get(id));
				}
			}
			System.out.println("Write pajek network format for the whole graph in "+outputDirectory+"/completepajekFile.net");
			graph.getGraphContent().writePajekFormat(new File(outputDirectory+"/completepajekFile.net"));
			
			graph.setNodesOfGraph(nodesOfGraph);
			graph.setEdgesOfGraph(edgesOfGraph);
			
			System.out.println("[Graph Generator] Generated basic graph with "+nodesOfGraph.size()+" nodes and "+edgesOfGraph.size()+" edges.");
			return graph;
		}
}
