package wdc.productcorpus.datacreator.OutputFilesCreator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import de.dwslab.dwslib.framework.Processor;

public class IdentifierInfoFetcher extends Processor<File>{
	
	HashMap<String, OutputOffer> offers;
	File tmpIDInfoDir;
	File productDir;
	File outputDir;
	Integer threads;
	

	public IdentifierInfoFetcher(HashMap<String, OutputOffer> offers, File tmpIDInfoDir, File productDir,
			File outputDir, Integer threads) {
		this.offers = offers;
		this.tmpIDInfoDir = tmpIDInfoDir;
		this.productDir = productDir;
		this.outputDir = outputDir;
		this.threads = threads;
	}
	
	@Override
	protected List<File> fillListToProcess() {
		List<File> files = new ArrayList<File>();
		for (File f : tmpIDInfoDir.listFiles()) {
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

	@Override
	protected void process(File object) throws Exception {
		
		BufferedReader br = new BufferedReader(new FileReader(object));
		
		HashMap<String,OutputOffer> offers_ = new HashMap<String, OutputOffer>();
		
		String line;

		while ((line = br.readLine()) != null) {
			
			String []lineParts= line.split("\\t");
			String offer_nodeID= lineParts[1];
			String offer_url = lineParts[2];
			String identifier_prop = lineParts[3];
			String identifier_value = lineParts[4];
			
			String offer_id = offer_nodeID+"\t"+offer_url;
			
			OutputOffer currentOffer = offers.get(offer_id);
			
			if (null == currentOffer) continue;
			
			HashSet<String> offer_id_values_current = currentOffer.getIdentifiers().get(identifier_prop);
			if (null == offer_id_values_current) {
				offer_id_values_current = new HashSet<String>();
				if (offers_.containsKey(offer_id)) {
					HashSet<String> offer_id_values_current_TMP = offers_.get(offer_id).getIdentifiers().get(identifier_prop);
					if (null != offer_id_values_current_TMP) offer_id_values_current.addAll(offer_id_values_current_TMP);
				}
				
			}
			offer_id_values_current.add(identifier_value);
			
			currentOffer.getIdentifiers().put(identifier_prop, offer_id_values_current);
			
			offers_.put(offer_id, currentOffer);
			
		}
		
		integrateOffers(offers_);
		br.close();
	}

	private synchronized void integrateOffers(HashMap<String, OutputOffer> offers_) {
		
		for (Map.Entry<String, OutputOffer> o:offers_.entrySet()) {
			
			HashMap<String, HashSet<String>> currentIdentifiers = offers.get(o.getKey()).getIdentifiers();
			for (Map.Entry<String, HashSet<String>> id_values:o.getValue().getIdentifiers().entrySet()) {
				HashSet<String> currentValues = currentIdentifiers.get(id_values.getKey());
				if (null == currentValues) currentValues = new HashSet<String>();
				
				currentValues.addAll(id_values.getValue());
				
				currentIdentifiers.put(id_values.getKey(), currentValues);
			}
			
			offers.get(o.getKey()).setIdentifiers(currentIdentifiers);
		}
		
	}
	
	@Override
	protected void beforeProcess() {
		System.out.println("[IdentifierInfoFetcher] Add identifier information to the offers.");
	}
	
	@Override
	protected void afterProcess() {
		System.out.println("[IdentifierInfoFetcher] Added identifier information about "+offers.size()+" offers.");

		DescriptionFetcher fetchDesc = new DescriptionFetcher(offers, productDir, outputDir, threads);
		fetchDesc.process();
	}
}
