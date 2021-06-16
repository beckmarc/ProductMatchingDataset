package wdc.productcorpus.v2.profiler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.InputUtil;
import wdc.productcorpus.util.DomainUtil;
import wdc.productcorpus.util.Histogram;
import wdc.productcorpus.util.SortMap;
import wdc.productcorpus.v2.model.Cluster;
import wdc.productcorpus.v2.model.ClusterStatic;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;

/**
 * @author Marc Becker
 * 
 */
public class MasterProfiler {


	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;	
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	@Parameter(names = "-pld", description = "Counts number of plds")
	private boolean pldProfiler;
	
	@Parameter(names = "-id", description = "Counts number of plds")
	private boolean idProfiler;
	
	@Parameter(names = "-property", description = "Counts number of plds")
	private boolean propertyProfiler;
	
	@Parameter(names = "-urlmax", description = "Counts number of plds")
	private int urlMax = Integer.MAX_VALUE;
	
	
	
	private HashMap<Integer,Integer> clusterIdToSize = new HashMap<Integer, Integer>();
	private HashMap<Integer, Integer> clusterSizeToOffersAmount = new HashMap<Integer, Integer>();
	private HashSet<String> plds = new HashSet<String>(); 
	private HashMap<String, Integer> offersPerPld = new HashMap<String,Integer>(); 
	private HashMap<String, Integer> offersPerUrl = new HashMap<String,Integer>(); 
	
		

 
	public void process() {
		if(idProfiler) {
			idProfiler();
		}
		if(pldProfiler) {
			pldProfiler();
		}
		if(propertyProfiler) {
			propertyProfiler();
		}
		
	}
	
	private void propertyProfiler() {
		PropertyProfiler pp = new PropertyProfiler(outputDirectory, inputDirectory, threads);
		pp.process();
	}

	public void pldProfiler() {
		HashSet<String> allUrls = new HashSet<String>();
		HashSet<String> urls = new HashSet<String>();
		String line;
		try {
			for(File f : inputDirectory.listFiles()) {
				if (!f.isDirectory() && f.getName().equals("offers.json")) { 
					BufferedReader br = InputUtil.getBufferedReader(f);
					int entityCount = 0;
					while ((line = br.readLine()) != null) {
						entityCount++;
						Entity e = EntityStatic.parseEntity(line);
						String pld = DomainUtil.getPayLevelDomainFromWholeURL(e.url);
						
						//url count
						allUrls.add(e.url);
						
						// pld count
						plds.add(pld);
						
						// offers per pld
						int count = offersPerPld.containsKey(pld) ? offersPerPld.get(pld) : 0;
						offersPerPld.put(pld, count + 1);		
						
						// offers per url
						count = offersPerUrl.containsKey(e.url) ? offersPerUrl.get(e.url) : 0;
						offersPerUrl.put(e.url, count + 1);	
						
						if(entityCount % 1000000 == 0) {
							PrintUtils.p("Parsed " + entityCount + " entities");
						}
						if(offersPerUrl.get(e.url) > urlMax) {
							urls.add(e.url);
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			Histogram<String> draw = new Histogram<String>("Distribution of offers per pld" , true, new FileWriter (new File (outputDirectory+"/offersPerPld.txt")));
			draw.drawDistr(1, offersPerPld);
			
			CustomFileWriter.writeTextToFile("urlsToCheck", outputDirectory, "", urls);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		PrintUtils.p("Number of plds:  " + plds.size() );
		PrintUtils.p("Number of Distinct Urls:  " + allUrls.size() );
}
	
	public void idProfiler() {
		int biggestCluster = 0;
		String line;
		try {
			for(File f : inputDirectory.listFiles()) {
				if (!f.isDirectory() && f.getName().equals("clusters.json")) { 
					BufferedReader br = InputUtil.getBufferedReader(f);
					int clusterCount = 0;
					while ((line = br.readLine()) != null) {
						clusterCount++;
						Cluster c = ClusterStatic.parseCluster(line);
						if(c.size > biggestCluster) {
							biggestCluster = c.size;
						}
						clusterIdToSize.put(c.id, c.size);
						if(clusterCount % 1000000 == 0) {
							PrintUtils.p("Parsed " + clusterCount + " clusters");
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	
		try {
			for(File f : inputDirectory.listFiles()) {
				if (!f.isDirectory() && f.getName().equals("offers.json")) { 
					BufferedReader br = InputUtil.getBufferedReader(f);
					int entityCount = 0;
					while ((line = br.readLine()) != null) {
						entityCount++;
						Entity e = EntityStatic.parseEntity(line);
						// increment values
						int clusterSize = clusterIdToSize.get(e.cluster_id);
						int count = clusterSizeToOffersAmount.containsKey(clusterSize)  ? clusterSizeToOffersAmount.get(clusterSize) : 0;
						clusterSizeToOffersAmount.put(clusterSize, count + 1);
						
						if(entityCount % 1000000 == 0) {
							PrintUtils.p("Parsed " + entityCount + " entities");
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		long lineCount = (long)0.0;
		
		PrintUtils.p(clusterSizeToOffersAmount.entrySet());
		
		for(Map.Entry<Integer, Integer> entry : clusterSizeToOffersAmount.entrySet()) {
			CustomFileWriter.writeLineToFile("offersPerClusterSize.txt", outputDirectory, ">" + entry.getKey() + " : " + entry.getValue() + " : " + ((entry.getKey()*(entry.getKey()-1))/2)*entry.getValue());
		}
	}

	
	
}
