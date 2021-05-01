package wdc.productcorpus.datacreator.OutputFilesCreator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import com.google.gson.JsonObject;

import de.dwslab.dwslib.framework.Processor;

public class IDClusterInfoFetcher extends Processor<File>{
	
	@Parameter(names = { "-clusterDir",
	"-clusterDirectory" }, required = true, description = "Folder which contains the component data", converter = FileConverter.class)
	private File clusterDir; 
	
	@Parameter(names = { "-tmpIDInfoDir",
		"-tmpIDInfoDirectory" }, required = true, description = "Folder which contains intermediate files with identifier information.", converter = FileConverter.class)
	private File tmpIDInfoDir;
	
	@Parameter(names = "-productDir", required = true, description = "Folder which contains the initial product files (wdc product subcorpus).", converter = FileConverter.class)
	private File productDir;
	
	@Parameter(names = "-outputDir", required = true, description = "Folder were the output files will be written to.", converter = FileConverter.class)
	private File outputDir;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	

	@Override
	protected List<File> fillListToProcess() {
		List<File> files = new ArrayList<File>();
		for (File f : clusterDir.listFiles()) {
			if (!f.isDirectory()) {
				files.add(f);
			}
		}
		return files;
	}
	
	@Override
	protected int getNumberOfThreads() {
		return this.threads;
	}
	
	//key of the offers is nodeid+"/t"+url
	HashMap<String, OutputOffer> offers = new HashMap<String, OutputOffer>();
	//key of the clusters id the cluster id (file name ithout extension)
	HashMap<String, OutputCluster> clusters = new HashMap<String, OutputCluster>();
	
	@Override
	protected void process(File object) throws Exception {
		
		BufferedReader br = new BufferedReader(new FileReader(object));
		HashMap<String, OutputOffer> offers_ = new HashMap<String, OutputOffer>();
		HashMap<String, OutputCluster> clusters_ = new HashMap<String, OutputCluster>();
		String line;

		while ((line = br.readLine()) != null) {
			
			String cluster_id = object.getName().replace(".txt", "");
			
			String []lineParts= line.split("\\t");
			String offer_nodeID = lineParts[0];
			String offer_url = lineParts[1];
			String [] cluster_identifiers = lineParts[3].replaceAll("\\[", "").replaceAll("\\]", "").replaceAll(" ","").split(",");
			
			String offer_id = offer_nodeID+"\t"+offer_url;

			//add to the cluster info
			OutputCluster currentCluster = clusters_.get(cluster_id);
			if (null == currentCluster) {
				currentCluster= new OutputCluster();
				currentCluster.setId(cluster_id);
				currentCluster.setOfferIDs(new HashSet<String>());
			}
			currentCluster.getOfferIDs().add(offer_id);	
			currentCluster.getIdentifiers().addAll(new HashSet<String>(Arrays.asList(cluster_identifiers)));
			
			clusters_.put(cluster_id, currentCluster);
			
			//add to the offers info
			OutputOffer currentOffer = offers_.get(offer_id);
			if (null == currentOffer) {
				currentOffer = new OutputOffer();
				currentOffer.setUrl(offer_url);
				currentOffer.setNodeID(offer_nodeID);
				currentOffer.setCluster_id(cluster_id);
			}
			else System.out.println("[IDClusterInfoFetcher] Every offer should actually appear only in one cluster. Please check.");
			
			offers_.put(offer_id, currentOffer);
		}
		
		integrateOffers(offers_);
		integrateClusters(clusters_);
		br.close();
	}

	private synchronized void integrateClusters(HashMap<String, OutputCluster> clusters_) {
		for (Map.Entry<String, OutputCluster> c: clusters_.entrySet()){
			if (clusters.containsKey(c.getKey())) System.out.println("[IDClusterInfoFetcher] Every cluster id should appear once. Please check.");
			clusters.put(c.getKey(), c.getValue());
		}
	
	}

	private synchronized void integrateOffers(HashMap<String, OutputOffer> offers_) {
		for (Map.Entry<String, OutputOffer> o: offers_.entrySet()){
			if (offers.containsKey(o.getKey())) System.out.println("[IDClusterInfoFetcher] Every offer id should appear once. Please check.");
			offers.put(o.getKey(), o.getValue());
		}
		
	}
	@Override
	protected void beforeProcess() {
		System.out.println("[IDClusterInfoFetcher] Add cluster info and offer id info.");
	}
	
	@Override
	protected void afterProcess() {
		
		writeClusterInfo();
		System.out.println("[IDClusterInfoFetcher] Added information about "+clusters.size()+" clusters and id information about "+offers.size()+" offers.");
		IdentifierInfoFetcher idfetcher = new IdentifierInfoFetcher(offers,tmpIDInfoDir, productDir,outputDir, threads);
		idfetcher.process();
		
	}

	private void writeClusterInfo() {
		
		try{
			BufferedWriter writer = new BufferedWriter (new FileWriter(outputDir.toString()+"/idClusterInfo.json",true));
			
			//convert to json	
			for (Map.Entry<String, OutputCluster> cluster:clusters.entrySet()) {
				
				JsonObject item = new JsonObject();
				item.addProperty("id", cluster.getValue().getId());
				item.addProperty("size", cluster.getValue().getClusterSize());
				item.addProperty("id_values", cluster.getValue().getIdentifiers().toString());

				writer.write(item.toString()+"\n");

			}
		
			
			writer.flush();
			writer.close();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
		
	}
	
	
	
}
