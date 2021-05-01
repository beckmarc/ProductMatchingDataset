package wdc.productcorpus.datacreator.ClusterCreator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.apache.commons.lang.StringUtils;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import com.google.common.net.InternetDomainName;
import com.google.gson.JsonObject;

import de.dwslab.dwslib.framework.Processor;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Node;
import de.wbsg.loddesc.util.DomainUtils;
import wdc.productcorpus.datacreator.ClusterCreator.utils.Component;
import wdc.productcorpus.datacreator.ClusterCreator.utils.GraphCleaner;
import wdc.productcorpus.datacreator.ClusterCreator.utils.GraphElement;
import wdc.productcorpus.datacreator.ClusterCreator.utils.GraphGenerator;
import wdc.productcorpus.datacreator.ClusterCreator.utils.Offer;
import wdc.productcorpus.datacreator.ClusterCreator.utils.ProductClass;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputCluster;
import wdc.productcorpus.util.Histogram;
import wdc.productcorpus.util.InputUtil;

public class GraphCreator extends Processor<File>{

	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "File to write the pajek graph.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	@Parameter(names = "-clean", description = "Indicate if the graph should be cleaned from vertices with small Clustering Coefficient.")
	private Boolean cleanSmallCC = false;
	
	@Parameter(names = "-filterOffersCount", description = "Indicate if the graph should be filtered from components with less than 10 offers.")
	private Boolean filterOffersCount = false;
	
	@Parameter(names = "-filterPLDCount", description = "Indicate if the graph should be filtered from components with less than 5 plds.")
	private Boolean filterPLDCount = false;
	
	@Parameter(names = "-saveComponentData", description = "Indicate if the component data should be saved.")
	private Boolean saveComponentData = false;
	
	private HashMap<String, ArrayList<String>> loadedEntities = new HashMap<String, ArrayList<String>>();
	private HashSet<String> uniqueIdentifiers = new HashSet<String>();
	
	private GraphElement graph = new GraphElement();
	//Graph<Node<ProductClass>, String> offersGraph = new Graph<Node<ProductClass>, String>();
	
	@Override
	protected List<File> fillListToProcess() {
		List<File> files = new ArrayList<File>();
		for (File f : inputDirectory.listFiles()) {
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
	
	public static InputStream getInputStream(File f) throws IOException {
		InputStream is;
		if (!f.isFile()) {
			throw new IOException("Inputfile is not a file but a directory.");
		}
		if (f.getName().endsWith(".gz")) {
			is = new GZIPInputStream(new FileInputStream(f));
		} else if (f.getName().endsWith(".zip")) {
			is = new ZipInputStream(new FileInputStream(f));
		
		} else {
			is = new FileInputStream(f);
		}
		return is;
	}
	@Override
	protected void process(File object){
		HashMap<String, ArrayList<String>> tempEntities = new HashMap<String, ArrayList<String>>();
		HashSet<String> tempIdentifiers = new HashSet<String>();
		
		try{

			BufferedReader br = InputUtil.getBufferedReader(object);
			String line="";

			while ((line = br.readLine()) != null) {
				String [] lineParts =line.split("\\t");
				String key = lineParts[1]+"\t"+lineParts[2];
				String idProp = lineParts[3];
				String idValue = lineParts[4];
				
				ArrayList<String> currentValues = tempEntities.get(key);
				if (null == currentValues) currentValues = new ArrayList<String>();
				currentValues.add(idProp+"||"+idValue);
				tempEntities.put(key, currentValues);
				
				tempIdentifiers.add(idValue);
					
			}
			
			br.close();
		}
		catch (Exception e){
			System.out.println(e.getMessage());
		}
		
		integrateLoadedEntities(tempEntities);
		integrateIdentifiers(tempIdentifiers);
	}
	
	private synchronized void integrateIdentifiers(HashSet<String> tempIdentifiers) {
		
		this.uniqueIdentifiers.addAll(tempIdentifiers);
		
	}

	private synchronized void integrateLoadedEntities(HashMap<String, ArrayList<String>> tempEntities) {
		
		for (String key : tempEntities.keySet()) {
			ArrayList<String> values = this.loadedEntities.get(key);
			if (null == values) values = new ArrayList<String>();
			values.addAll(tempEntities.get(key));

			this.loadedEntities.put(key, values);
		}
		
	}
	
	@Override
	protected void afterProcess() {
		try{
			System.out.println("Loaded "+loadedEntities.size()+" unique entities. ");
			HashSet<Offer> offers = getOffers();
			
			System.out.println("Loaded "+offers.size()+" offers. ");
			
			System.out.println("[Graph Creator] Loaded "+uniqueIdentifiers.size()+" unique identifier values.");
			GraphGenerator generate = new GraphGenerator();
			this.graph = generate.createBasicGraph(offers, uniqueIdentifiers, outputDirectory);

			GraphCleaner clean = new GraphCleaner();
			this.graph = clean.cleanGraph(this.graph, cleanSmallCC, filterOffersCount, filterPLDCount, outputDirectory);

			//write info
			writeComponentInfo(this.graph.getComponents());
			if (saveComponentData)
				writeOffersPerComponent(this.graph.getComponents());
			
			//write nodes to component id
			writeOffersToClusterID(this.graph.getComponents());
			writeClustersInfo(this.graph.getComponents());
			
			writeCorpusInfo();
	
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
	}

	
	
	

	
	private HashSet<Offer> getOffers() {
		
		HashSet<Offer> offers = new HashSet<Offer>();
		
		int counter=0;
		//create offers
		for (Map.Entry<String, ArrayList<String>> e:loadedEntities.entrySet()) {
			counter++;
			if (counter%500000==0) System.out.println("Loaded "+counter+" from the "+loadedEntities.size()+" loaded entities.");
			Offer offer = new Offer();
			offer.setKey(e.getKey());
			HashMap<String, ArrayList<String>> propValues = new HashMap<String, ArrayList<String>>();
			
			for (String v:e.getValue()) {
				String parts [] = v.split("\\|\\|");
				ArrayList<String> currentValues = propValues.get(parts[0]);
				if (null == currentValues) currentValues = new ArrayList<String>();
				currentValues.add(parts[1]);
				propValues.put(parts[0], currentValues);
			}
			offer.setPropValue(propValues);
			offers.add(offer);
		}
		return offers;
	}
	
	private void writeCorpusInfo() throws IOException {
		HashMap<String, HashSet<Offer>> offersPerUrl = new HashMap<String, HashSet<Offer>>();
		HashMap<String, HashSet<Offer>> offersPerTLD = new HashMap<String, HashSet<Offer>>();
		
		Integer nullDomains = 0;
		System.out.println("Write information about "+this.graph.getOffersCount()+" offers.");
		
		for (Component c: this.graph.getComponents()) {
			for (Offer o: c.getOffers()) {
				String url = o.getKey().split("\\t")[1];
				String domain = null;
				InternetDomainName internetDomain = null;
				String tld = null;
				
				try {
					domain = DomainUtils.getDomain(url);
					internetDomain = InternetDomainName.from(domain);
					tld = internetDomain.publicSuffix().toString();
					
					if (null == tld) {
						nullDomains++;
						tld = "notcomputed";
					}
				}
				catch (Exception e) {
					nullDomains++;
					tld = "notcomputed";
				}
				
				
				HashSet<Offer> offersOfUrl = offersPerUrl.get(url);
				if (null == offersOfUrl) offersOfUrl=new HashSet<Offer>();
				offersOfUrl.add(o);
				offersPerUrl.put(url, offersOfUrl);
				
				HashSet<Offer> offersOfTLD = offersPerTLD.get(tld);
				if (null == offersOfTLD) offersOfTLD=new HashSet<Offer>();
				offersOfTLD.add(o);
				offersPerTLD.put(tld, offersOfTLD);
			}
			
		}
		
		HashMap<String,Integer> offersPerUrlCount = new HashMap<String, Integer>();
		HashMap<String,Integer> offersPerTLDCount = new HashMap<String, Integer>();

		for (Map.Entry<String, HashSet<Offer>> e: offersPerTLD.entrySet())
			offersPerTLDCount.put(e.getKey(), e.getValue().size());
		
		for (Map.Entry<String, HashSet<Offer>> e: offersPerUrl.entrySet())
			offersPerUrlCount.put(e.getKey(), e.getValue().size());
		
		System.out.println("Could not extract tld info from "+nullDomains+" urls.");

		
		System.out.println("Draw corpus histograms");
		
		Histogram<String> draw = new Histogram<String>("Distribution of offers per url" , true, new FileWriter (new File (outputDirectory+"/offersPerUrl.txt")) );
		draw.drawDistr(1, offersPerUrlCount);
		
		System.out.println("Offers per tld");
		System.out.println(offersPerTLDCount.toString());
		
		draw = new Histogram<String>("Distribution of offers per tld" , true, new FileWriter (new File (outputDirectory+"/offersPerTLD.txt") ));
		draw.drawDistr(1, offersPerTLDCount);

	}
	
	
	private void writeOffersPerComponent(HashSet<Component> components) throws IOException {
		
		new File(outputDirectory.getPath()+"/componentData").mkdir();

		for (Component c:components) {
			BufferedWriter writer  = new BufferedWriter(new FileWriter(outputDirectory+"/componentData/"+c.getComponentID().toString()+".txt"));
			for (Offer o:c.getOffers()) {
			
				String concatProps= StringUtils.join(o.getPropValue().keySet(), ", ");
				String concatValues = StringUtils.join(o.getPropValue().values(),", ");
				writer.write(o.getKey().split("\\t")[0]+"\t"+o.getKey().split("\\t")[1]+"\t"+ concatProps+"\t"+concatValues+"\n");
			}
			
			
			writer.flush();
			writer.close();
		}
		
	}
	
	private void writeClustersInfo(HashSet<Component> components) throws IOException {
		BufferedWriter writer  = new BufferedWriter(new FileWriter(outputDirectory.getPath()+"/clusters.json"));


		for (Component c:components) {
			Integer compID = c.getComponentID();
			ArrayList<String> values= new ArrayList<String>();
			for (Node<ProductClass> p: c.getNodes()) {
				values.add(p.getData().getId());
			}
			
				
			JsonObject item = new JsonObject();
			item.addProperty("id", compID);
			item.addProperty("size", values.size());
			item.addProperty("id_values",values.toString());

			writer.write(item.toString()+"\n");

		
		}
		
		writer.flush();
		writer.close();
	}
	
	
	private void writeOffersToClusterID(HashSet<Component> components) throws IOException {
		
		BufferedWriter writer  = new BufferedWriter(new FileWriter(outputDirectory.getPath()+"/offers.json"));


		for (Component c:components) {

			for (Offer o:c.getOffers()) {
				
				JsonObject item = new JsonObject();
				item.addProperty("url", o.getKey().split("\\t")[1]);
				item.addProperty("nodeID", o.getKey().split("\\t")[0] );
				item.addProperty("cluster_id",c.getComponentID());

				writer.write(item.toString()+"\n");
			}
			
			
			writer.flush();
		}
		
		writer.close();

		
	}


	private void writeComponentInfo(HashSet<Component> components) throws IOException {
		

		System.out.println("Write information about "+components.size()+" connected components.");
		
		//create histogram for components sizes in terms of nodes and offers
		HashMap<Integer, Integer> compsize = new HashMap<Integer, Integer>();
		
		HashMap<Integer, Integer> compOffersCount = new HashMap<Integer, Integer>();
		
		for (Component c: components) {
			compsize.put(c.getComponentID(), c.getNodes().size());			
			compOffersCount.put(c.getComponentID(), c.getOffersCount());
		}
		
		Histogram<Integer> draw = new Histogram<Integer>("Distribution of nodes per connected component",true, new FileWriter(new File (outputDirectory+"/nodesPercomponentHistogram.txt")));
		draw.drawDistr(1, compsize);
		
		draw = new Histogram<Integer>("Distribution of offers per connected component",true, new FileWriter( new File (outputDirectory+"/offersPercomponentHistogram.txt")));
		draw.drawDistr(1, compOffersCount);
		
	}
}
