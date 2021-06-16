package wdc.productcorpus.v2.profiler;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.InputUtil;
import ldif.runtime.Quad;
import wdc.productcorpus.util.DomainUtil;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.model.ParseEntityResult;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;
import wdc.productcorpus.v2.util.QuadParser;

public class CorpusProfiler extends Processor<File>{
	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
		
	
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
	
	
	
	ArrayList<SchemaOrg> schemas = new ArrayList<SchemaOrg>() {{
	    add(new SchemaOrg(true, "webpage", true));
	    add(new SchemaOrg(true, "searchresultspage", true));
	    add(new SchemaOrg(true, "realestatelisting", true));
	    add(new SchemaOrg(true, "collectionpage", true));
	    add(new SchemaOrg(true, "checkoutpage", true));
	    add(new SchemaOrg(true, "aboutpage", true));
	    add(new SchemaOrg(true, "contactpage", true));
	    add(new SchemaOrg(true, "faqpage", true));
	    add(new SchemaOrg(true, "medicalwebpage", true));
	    add(new SchemaOrg(true, "profilepage", true));
	    add(new SchemaOrg(true, "qapage", true));
	    add(new SchemaOrg(true, "itempage", true));
	    
	    add(new SchemaOrg(true, "offer", false));
	    add(new SchemaOrg(true, "product", false));
	    
	    add(new SchemaOrg(false, "offers", false));
	    add(new SchemaOrg(false, "itemoffered", false));
	    add(new SchemaOrg(false, "mainentity", false));
	    add(new SchemaOrg(false, "mainentityofpage", false));
	    add(new SchemaOrg(false, "similar", false));
	    add(new SchemaOrg(false, "related", false));
	}};
	
	ArrayList<SchemaOrg> typeProp = new ArrayList<SchemaOrg>() {{
	    add(new SchemaOrg("product", new String[]{"name","url","brand","image","description","offers",
	    		"productid","sku","mpn","gtin13","identifier","gtin8","gtin12","gtin14","serialnumber","gtin","mainentityofpage"}));
	    add(new SchemaOrg("offer", new String[]{"price","pricecurrency","availability",
	    		"productid","sku","mpn","gtin13","identifier","gtin8","gtin12","gtin14","serialnumber","gtin","mainentityofpage"}));
	}};
	
	Map<String, Long> freq_ = new HashMap<String, Long>();
	Set<String> plds = Collections.synchronizedSet(new HashSet<String>());
	HashMap<String, HashSet<String>> pldsPerProps = new HashMap<String, HashSet<String>>();
	
	private long errorCount = 0;
	private long entityCount = 0;		
	private long urlCount = 0;
	
	@Override
	protected void process(File object1) throws Exception {
		
		Map<String, Long> freq = new HashMap<String, Long>();
		HashSet<String> plds = new HashSet<String>();
		HashMap<String, HashSet<String>> pldsPerProps = new HashMap<String, HashSet<String>>();
		
		ArrayList<Quad> myQuads = new ArrayList<Quad>();
		
		long errorCount = 0;
		long entityCount = 0;	
		long urlCount = 1; // one more because of last lines
		
		BufferedReader br = InputUtil.getBufferedReader(object1);
		QuadParser qp = new QuadParser(true);
		String currentLine="";
		List<Quad> quads = new ArrayList<Quad>();
		String currentEntity = "";
		long lines = 0;
		String currentUrl = "";
		
		while (br.ready()) {
			try {		
				currentLine = br.readLine(); 
				Quad q = qp.parseQuadLine(currentLine);
				
				String pld = DomainUtil.getPayLevelDomainFromWholeURL(q.graph());
				plds.add(pld);
				
				if(!q.graph().equals(currentUrl)) { // count urls
					urlCount++;
				}
				currentUrl = q.graph();
				
				lines++;
				if(lines % 1000000 == 0) {
					PrintUtils.p("Parsed " + lines + " from File " + object1.getName());
				}
				
				String predicate = q.predicate().toLowerCase();
				String object = q.value().toString().toLowerCase();
				
				if(predicate.contains("/mainentityofpage")) {
					myQuads.add(q);
				}
				
				// count all properties and values
				for(SchemaOrg so : schemas) {
					if(so.isType && object.contains(so.key) || !so.isType && predicate.contains(so.key)) {
						if(so.isPage && noMainPage(q)) {
							continue;
						}
						long count = freq.containsKey(so.key) ? freq.get(so.key) : 0;
						freq.put(so.key, count + 1);
					}
				}
					
				if (q != null && q.subject().toString().equals(currentEntity)) {
					quads.add(q);
				} else {
					if (quads.size() > 0) {
						for(SchemaOrg so : typeProp) { // for each type / property combination
							if(quads.get(0).value().toString().toLowerCase().contains(so.type)) { // check if type is present on first
								for(Quad entityQuad : quads) { // check each quad then
									for(String property : so.properties) { // for each quad check if it has any of the properties
										if(entityQuad.predicate().toLowerCase().contains(property)) { // if predicate has the property value
											
											String key = so.type + "/" + property; // key to identify the type / property
											// over all count of properties
											long count = freq.containsKey(key) ? freq.get(key) : 0;
											freq.put(key, count + 1);
											
											if(!pldsPerProps.containsKey(key))
											pldsPerProps.put(key, new HashSet<String>());
											pldsPerProps.get(key).add(pld);
										}
									}
								}
							}
						}
						entityCount++;		
					}
					quads.clear();
					quads.add(q);
					currentEntity = q.subject().toString();
					
					
				}
			} catch (Exception e) {
				errorCount++;
			}
		}
		// process once more for the last quads
		if (quads.size() > 0) {
			for(SchemaOrg so : typeProp) {
				if(quads.get(0).value().toString().toLowerCase().contains(so.type)) {
					for(Quad entityQuad : quads) {
						for(String property : so.properties) {
							if(entityQuad.predicate().toLowerCase().contains(property)) { // if predicate has the property value
								String key = so.type + "/" + property; // key to identify the type / property
								long count = freq.containsKey(key) ? freq.get(key) : 0;
								freq.put(key, count + 1);
								
								if(!pldsPerProps.containsKey(key))
								pldsPerProps.put(key, new HashSet<String>());
								pldsPerProps.get(key).add(DomainUtil.getPayLevelDomainFromWholeURL(quads.get(0).graph()));
							}
						}
					}
				}
			}
			entityCount++;	
		}
		
		this.plds.addAll(plds);
		pldMappings(pldsPerProps);
		mergeCounters(freq);
		updateCounters(entityCount, errorCount, urlCount);
		
		for(Quad q : myQuads) {
			PrintUtils.p(q.toNQuadFormat());
		}
		
	
		br.close();
		pldsPerProps.clear();
	}
	
	private synchronized void mergeCounters(Map<String, Long> freq) {
		for(Map.Entry<String, Long> entry : freq.entrySet()) {
			long count = freq_.containsKey(entry.getKey()) ? freq_.get(entry.getKey()) + entry.getValue() : entry.getValue();
			freq_.put(entry.getKey(), count);
		}
	}
	
	/**
	 * Merges mapping of plds by taking the size of them
	 * @param pldsPerProps
	 */
	private synchronized void pldMappings(HashMap<String, HashSet<String>> pldsPerProps) {
		for(Map.Entry<String, HashSet<String>> entry : pldsPerProps.entrySet()) {
			if(!this.pldsPerProps.containsKey(entry.getKey()))
			this.pldsPerProps.put(entry.getKey(), new HashSet<String>());
			this.pldsPerProps.get(entry.getKey()).addAll(entry.getValue());
		}
	}
	
	private synchronized void updateCounters(long entityCount, long errorCount, long urlCount) {
		this.entityCount += entityCount;
		this.errorCount += errorCount;
		this.urlCount += urlCount;
	}
	
	private boolean noMainPage(Quad q) {
		if(q.subject().toString().contains(q.graph()) || q.subject().isBlankNode()) {
			return false;
		}
		else {
			//PrintUtils.p(q);
			return true;
		}
	}

	@Override
	protected void afterProcess() {	
		HashMap<String, Integer> pldsCountPerProps = new HashMap<String, Integer>();
		for(Map.Entry<String, HashSet<String>> entry : pldsPerProps.entrySet()) {
			pldsCountPerProps.put(entry.getKey(), entry.getValue().size());
		}
		PrintUtils.p(freq_);
		PrintUtils.p(pldsCountPerProps);
		CustomFileWriter.clearFile("corpus-profile.txt", outputDirectory);
		CustomFileWriter.writeLineToFile("corpus-profile.txt" , outputDirectory, "entities : " + entityCount + "\n" +
				"Url count : " + urlCount + "\n" +
				"PLD count : " + plds.size());
		CustomFileWriter.writeLineToFile("corpus-profile.txt", outputDirectory, "-------------------------PLD-Stats---------------------------------------------------------------------------");
		CustomFileWriter.writeKeyValuesToFile("corpus-profile.txt" , outputDirectory, pldsCountPerProps);
		CustomFileWriter.writeLineToFile("corpus-profile.txt", outputDirectory, "-------------------------General stats---------------------------------------------------------------------------");
		CustomFileWriter.writeKeyValuesToFile("corpus-profile.txt" , outputDirectory, freq_);
		PrintUtils.p("Lines that could not be parsed: " + errorCount);
		PrintUtils.p("Wrote output to File: " + outputDirectory.getAbsolutePath());
	}


}
