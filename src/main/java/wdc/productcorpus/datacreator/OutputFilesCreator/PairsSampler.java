package wdc.productcorpus.datacreator.OutputFilesCreator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.stream.Collectors;


public class PairsSampler {
	
	File offers = new File ("");
	File clusters = new File ("C:\\Users\\User\\Desktop\\Cell_Phones_and_Accessories_medium_clusters");;
	File outputDir = new File ("C:\\Users\\User\\Desktop");
	int sampleSize = 50;
	
	ArrayList<String> categories = new ArrayList<String>(){{
		add("Office_Products");
		add("Jewelry");
		add("Clothing");
		add("Home_and_Kitchen");
		add("Cell_Phones_and_Accessories");
		add("Beauty");
	}};
	
	public static void main (String [] args) throws IOException {
		
		PairsSampler sample = new PairsSampler();
		
		if (args.length>0) {
			sample.offers = new File (args[0]);
			sample.clusters = new File (args[1]);
			sample.outputDir = new File (args[2]);
		}
		
		sample.generatePairs();
	}
	
	public void generatePairs() throws IOException {
		
		//import clusters
		ClusterImporter importclusters = new ClusterImporter(clusters);
		HashSet<OutputCluster> clusters = importclusters.importClusters(true);
		
		System.out.println("Imported "+clusters.size()+" clusters");
		
		//import offers
		DataImporter importOffers = new DataImporter(offers);
		HashMap<String, HashSet<OutputOffer>> offers = importOffers.importSimpleOfferWClusterFilter(clusters);
		System.out.println("Imported "+offers.size()+ " offers");
		
		for (String category:categories) {
			System.out.println("Category:"+category);
			List<OutputCluster> cluster_of_cat = clusters.stream().filter(c -> c.getCategory().equals(category)).collect(Collectors.toList());
			
			//split into the different sizes			
			System.out.println("Get small clusters");
			List<OutputCluster> cluster_of_cat_small = cluster_of_cat.stream().filter(c-> (c.getSizeInOffers()>2 && c.getSizeInOffers()<6)).collect(Collectors.toList());
			System.out.println(cluster_of_cat_small.size()+" small clusters of category "+category);
			ArrayList<Entry<OutputOffer,OutputOffer>> sampleOfferPairs_small = sampleFromCluster(cluster_of_cat_small,offers);
			writeSample(category, "small", sampleOfferPairs_small);
			
			System.out.println("Get medium clusters");
			List<OutputCluster> cluster_of_cat_medium = cluster_of_cat.stream().filter(c-> (c.getSizeInOffers()>5 && c.getSizeInOffers()<81)).collect(Collectors.toList());
			System.out.println(cluster_of_cat_medium.size()+" medium clusters of category "+category);
			ArrayList<Entry<OutputOffer,OutputOffer>> sampleOfferPairs_medium = sampleFromCluster(cluster_of_cat_medium,offers);
			writeSample(category, "medium", sampleOfferPairs_medium);
			
			System.out.println("Get large clusters");
			List<OutputCluster> cluster_of_cat_large = cluster_of_cat.stream().filter(c-> (c.getSizeInOffers()>80)).collect(Collectors.toList());
			System.out.println(cluster_of_cat_large.size()+" large clusters of category "+category);
			ArrayList<Entry<OutputOffer,OutputOffer>> sampleOfferPairs_large = sampleFromCluster(cluster_of_cat_large,offers);
			writeSample(category, "large", sampleOfferPairs_large);
					
			System.out.println("Finished with category "+category);
		}
		
	}

	private void writeSample(String category, String size,
			ArrayList<Entry<OutputOffer, OutputOffer>> sampleOfferPairs) throws IOException {
		
		File outputFile = new File (outputDir+"//"+category+"_"+size+"sample.txt");
		FileWriter writer = new FileWriter(outputFile);
		
		for (Entry<OutputOffer,OutputOffer> pair:sampleOfferPairs) {
			writer.write(pair.getKey().getCluster_id()+";"+pair.getKey().getNodeID()+"|"+pair.getKey().getUrl()+";"+pair.getValue().getNodeID()+"|"+pair.getValue().getUrl()+"\n");
		}
		
		writer.flush();
		writer.close();
		
	}

	private ArrayList<Entry<OutputOffer, OutputOffer>> sampleFromCluster(
			List<OutputCluster> cluster_of_cat_size, HashMap<String, HashSet<OutputOffer>> offers) {
		
		System.out.println("Get relevant clusters");
		Map<String, ArrayList<OutputOffer>> offersGroupedByCluster =  offers.entrySet() 
		          .stream() 
		          .filter(map -> cluster_of_cat_size.contains(new OutputCluster(map.getKey()))) 
		          .collect(Collectors.toMap(map -> map.getKey(), map -> new ArrayList<OutputOffer>(map.getValue())));
		
		System.out.println("Generate all pairs");
		ArrayList<Entry<OutputOffer,OutputOffer>> allPairs = new ArrayList<>();
		
		for (ArrayList<OutputOffer> offersOfSameCluster:offersGroupedByCluster.values()){
			Collections.shuffle(offersOfSameCluster);
			
			int offersCount = 0;
			for (int i=0;i<offersOfSameCluster.size();i++){
				for (int j=i+1; j<offersOfSameCluster.size();j++){
					offersCount++;
					if (offersCount>500) {
						i = offersOfSameCluster.size();
						j = offersOfSameCluster.size();
						break;
					}
					allPairs.add(new AbstractMap.SimpleEntry<OutputOffer, OutputOffer>(offersOfSameCluster.get(i),offersOfSameCluster.get(j)));
				}
			}
		}
		
		System.out.println("Generated "+allPairs.size()+" pairs (complete)");
		//sample
		System.out.println("Sample "+sampleSize+" pairs");
		ArrayList<Entry<OutputOffer,OutputOffer>> sampledPairs = new ArrayList<>();
		
		if (allPairs.size()>0) {
			HashSet<Integer> randomIndex = new HashSet<Integer>();
			
			Random rand = new Random();
			
		
			while (randomIndex.size()<sampleSize) {
				randomIndex.add(rand.nextInt(allPairs.size()-1));
			}
			
			for (Integer index: randomIndex)
				sampledPairs.add(allPairs.get(index));
		}
		
		
		return sampledPairs;
	}
	
	
			
}
