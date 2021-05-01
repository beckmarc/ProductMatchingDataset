package wdc.productcorpus.datacreator.OutputFilesCreator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import de.dwslab.dwslib.framework.Processor;
import ldif.local.datasources.dump.QuadFileLoader;
import ldif.runtime.Quad;
import wdc.productcorpus.util.InputUtil;

public class DescriptionFetcher extends Processor<File> {
	
	public static void main(String args[]) {
		
		DescriptionFetcher fetch = new DescriptionFetcher();
		if (args.length>0) {
			 fetch.offersFile = new File(args[0]);
			 fetch.productDir = new File(args[1]);
			 fetch.outputDir = new File(args[2]);
			 fetch.threads = Integer.parseInt(args[3]);
			
		}
		fetch.process();
	}
	
	
	HashMap<String,OutputOffer> corpusOffers = new HashMap<String,OutputOffer>();
	File productDir = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\IdentifierExtractor\\input");
	File outputDir =new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\output");
	File offersFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\transform");
	Integer threads = 1;
	
	
	ArrayList<String> textualProperties = new ArrayList<String>() {{
	    add("/brand");
	    add("/name");
	    add("/title");
	    add("/description");
		add("/price");
		add("/priceCurrency");
		add("/image");
		add("/availability");
		add("/manufacturer");

	}};
	
	public DescriptionFetcher(){}
	
	public DescriptionFetcher(HashMap<String, OutputOffer> offers, File productDir, File outputDir, Integer threads) {
		for (OutputOffer o: offers.values()) {
			this.corpusOffers.put(o.getNodeID()+o.getUrl(), o);
		}
		this.productDir = productDir;
		this.outputDir = outputDir;
		this.threads = threads;
	}
	
	@Override
	protected List<File> fillListToProcess() {
		List<File> files = new ArrayList<File>();
		if (productDir.isDirectory()) {
			for (File f : productDir.listFiles()) {
				if (!f.isDirectory()) {
					files.add(f);
				}
			}
		}
		else files.add(productDir);
		return files;
	}
	
	@Override
	protected int getNumberOfThreads() {
		return this.threads;
	}
	

	@Override
	protected void process(File object) throws Exception {
		HashMap<String, OutputOffer> offers_= new HashMap<String, OutputOffer>();
		QuadFileLoader qfl = new QuadFileLoader();
		BufferedReader br = InputUtil.getBufferedReader(object);
		String line ="";
		while ((line=br.readLine())!= null) {
			try {
				Quad q = qfl.parseQuadLine(line);
				String id = q.subject().toString()+q.graph().toString();
				//OutputOffer tmp_offer = new OutputOffer(q.graph(), q.subject().toString());
						
				
				if (corpusOffers.containsKey(id)) {
								
					OutputOffer currentOffer = corpusOffers.get(id);
					
					String predicate = q.predicate();
					String value = q.value().toString();
					
					for (String textProp:textualProperties) {
						//get the text of literal of object of the predicates we have defined
						if (predicate.toString().contains(textProp) && value.startsWith("\"")) {
							
							HashSet<String> currentValues = currentOffer.getDescProperties().get(textProp);
							if (null == currentValues) {
								currentValues = new HashSet<String>();

								if (offers_.containsKey(id)) {
									HashSet<String> currentTMPValues = offers_.get(id).getDescProperties().get(textProp);
									if (null != currentTMPValues)
										currentValues.addAll(currentTMPValues);									
								}
											
							}
							currentValues.add(value);
							currentOffer.getDescProperties().put(textProp, currentValues);
						}
					}
					
					offers_.put(id, currentOffer);
				}
			}
			catch(Exception e) {
				System.out.println(e.getMessage());
			}
		}
		
		integrateOffers(offers_);
	}
	
	private synchronized void integrateOffers(HashMap<String, OutputOffer> offers_) {
		for (Map.Entry<String, OutputOffer> o:offers_.entrySet()) {
			OutputOffer tmp_offer = corpusOffers.get(o.getKey());
			HashMap<String,HashSet<String>> descProperties = tmp_offer.getDescProperties();

			for (Map.Entry<String, HashSet<String>> desc: o.getValue().getDescProperties().entrySet()) {
				
				HashSet<String> currentValues = tmp_offer.getDescProperties().get(desc.getKey());
				if (null == currentValues) currentValues = new HashSet<String>();
				currentValues.addAll(desc.getValue());
				
				descProperties.put(desc.getKey(),currentValues);
			}
			
			corpusOffers.get(o.getKey()).setDescProperties(descProperties);
		}
		
	}

	@Override
	protected void beforeProcess() {
		System.out.println("[DescriptionFetcher] Add schema.org properties information to the corpus offers.");
		//load offers 
		DataImporter load = new DataImporter(offersFile);
		HashSet<OutputOffer> offers = load.importOffers();
		for(OutputOffer o:offers)
			corpusOffers.put(o.getNodeID()+o.getUrl(), o);
	
		System.out.println("Loaded "+corpusOffers.size()+" offers");
	}
	
	@Override
	protected void afterProcess() {
	
		writeOfferInfo();
		System.out.println("[DescriptionFetcher] Done. Added description info to "+corpusOffers.size()+" offers.");
	}

	private void writeOfferInfo() {
		try{
			BufferedWriter writer = new BufferedWriter (new FileWriter(outputDir.toString(),true));
			
			//convert to json	
			for (OutputOffer offer:corpusOffers.values()) {

				JsonObject offer_json = offer.toJSONObject(true);

				writer.write(offer_json.toString()+"\n");

			}
		
			writer.flush();
			writer.close();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
		
	}
}
