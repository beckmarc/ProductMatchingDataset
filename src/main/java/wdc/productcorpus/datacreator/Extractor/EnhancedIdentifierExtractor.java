package wdc.productcorpus.datacreator.Extractor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.InputUtil;
import ldif.local.datasources.dump.QuadFileLoader;
import ldif.runtime.Quad;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;

/**
 * @author Anna Primpeli
 * Looks at the product file and extract all entities that have some sort of identifier together 
 * with their descriptive properties and parent relations and parent item propertiees
 * The inpur product file should be ordered by URL
 */
public class EnhancedIdentifierExtractor extends Processor<File>{
	
	File productDir = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\check");
	File outputDir = new File ("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp");
	int threads =1;
	
	public static void main(String args[]) {
		EnhancedIdentifierExtractor extract = new EnhancedIdentifierExtractor();
		if (args.length>0) {
			extract.productDir=new File(args[0]);
			extract.outputDir = new File(args[1]);
			extract.threads = Integer.parseInt(args[2]);
		}
		extract.process();
	}
	
	ArrayList<String> identifiers = new ArrayList<String>() {{
	    add("/gtin12");
	    add("/gtin13");
	    add("/gtin14");
	    add("/gtin8");
	    add("/mpn");
	    add("/productID");
	    add("/sku");
	    add("/identifier");
	}};
	
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
		// maintain thread-internal maps to reduce waiting time for other
		
		int lineCount = 0;
		HashMap<String, ArrayList<Quad>> quadsOfURL = new HashMap<String,ArrayList<Quad>>();
		HashMap<String, Boolean> URLhasID = new HashMap<String,Boolean>();
		
		ArrayList<OutputOffer> offers = new ArrayList<OutputOffer>();
		// quadloader
		QuadFileLoader qfl = new QuadFileLoader();
		// read the file
		BufferedReader br = InputUtil.getBufferedReader(object);
		String line ="";
		while ((line=br.readLine())!=null) {
			try {
				lineCount++;
				if (lineCount%1000000==0) System.out.println("Parsed from "+object.getName()+ " lines "+lineCount);
				Quad q = qfl.parseQuadLine(line);
				String url = q.graph().toString();

				ArrayList<Quad> exQuads = quadsOfURL.get(url);
				if (null == exQuads){
					exQuads = new ArrayList<Quad>();
					URLhasID.put(url, false);
				}
				exQuads.add(q);
				quadsOfURL.put(url, exQuads);
				
				for (String id:identifiers){			
					if (q.predicate().contains(id)) {
						URLhasID.put(url, true);
					}
				}
				
				
			} catch (Exception e) {
				// TODO make this an option
				e.printStackTrace();
			}
		}
		
		br.close();
		//now get offers out of the grouped quads
		for (Map.Entry<String, ArrayList<Quad>> quads:quadsOfURL.entrySet()) {
			ArrayList<OutputOffer> offersofurl =  quadsToOutputOffers(quads.getValue());
			offers.addAll(offersofurl);
		}


		// integrate the data
		//or simply write?
		write(offers,object);
	}

	private void write(ArrayList<OutputOffer> offers, File object) throws IOException {
		
		System.out.println("Write results of file "+object.getName());
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputDir+"/"+object.getName()+"_extractedOffers.json"));
		
		for (OutputOffer o: offers) {
			writer.write(o.toJSONObject(true)+"\n");
		}
		writer.flush();
		writer.close();
	}

	/**
	 * @param quads
	 * @param currentURL
	 * Take all the quads from one url and extract all the info we need
	 */
	private ArrayList<OutputOffer> quadsToOutputOffers(List<Quad> quads) {
		
		HashMap<String,OutputOffer> possibleoffers = new HashMap<String,OutputOffer>();
		
		HashMap<String, Quad> relatedNodes = new HashMap<String, Quad>();
		
		for (Quad q:quads){
					
			String nodeid = q.subject().toString();
			String url = q.graph().toString();
			OutputOffer tmpOffer = possibleoffers.get(nodeid);
			if (null == tmpOffer) tmpOffer = new OutputOffer(url, nodeid);
			

			for (String id:identifiers){			
				if (q.predicate().contains(id)) {
					HashSet<String> exValues = tmpOffer.getIdentifiers().get(id);
					if (null == exValues) exValues = new HashSet<String>();
					exValues.add( normalizeValue(q.value().toString()));
					exValues.add(q.value().toString());
					
					HashSet<String> exValues_non_norm = tmpOffer.getNonprocessedIdentifiers().get(id);
					if (null == exValues_non_norm) exValues_non_norm = new HashSet<String>();
					exValues_non_norm.add(q.value().toString());
					exValues_non_norm.add(q.value().toString());
					
					tmpOffer.getIdentifiers().put(id, exValues);
					tmpOffer.getNonprocessedIdentifiers().put(id, exValues_non_norm);
				}
			}
			
			for (String textProp:textualProperties) {
				//get the text of literal of object of the predicates we have defined
				if (q.predicate().toString().contains(textProp) && q.value().toString().startsWith("\"")) {
					HashSet<String> exValues = tmpOffer.getDescProperties().get(textProp);
					if (null == exValues) exValues = new HashSet<String>();
					if (textProp.equals("/image"))exValues.add( q.value().toString());
					//else exValues.add( normalizeText(q.value().toString()));
					else exValues.add(q.value().toString());
					tmpOffer.getDescProperties().put(textProp, exValues);		
				}
					
				
			}
			if (q.value().isResource() && q.predicate().toString()!="http://www.w3.org/1999/02/22-rdf-syntax-ns#type") {
				relatedNodes.put(q.value().toString(), q);
			}
			
			
			possibleoffers.put(nodeid, tmpOffer);
			
			
		}
		
		HashMap<String,OutputOffer> offers = new HashMap<String,OutputOffer>();
		//and now filter
		for (Map.Entry<String, OutputOffer> offer: possibleoffers.entrySet()) {
			
			if (!offer.getValue().getIdentifiers().isEmpty()) {
				//add parent info if existing
				if(relatedNodes.containsKey(offer.getKey())) {
					String parentID= relatedNodes.get(offer.getKey()).subject().toString();
					String relationToParent = relatedNodes.get(offer.getKey()).predicate().toString();
					HashMap<String,HashSet<String>> parentDescProps = possibleoffers.get(parentID).getDescProperties();
					
					offer.getValue().setParentNodeID(parentID);
					offer.getValue().setParentdescProperties(parentDescProps);
					offer.getValue().setPropertyToParent(relationToParent);
				}
				
				
				offers.put(offer.getKey(),offer.getValue());
			}
		}
		return new ArrayList<OutputOffer>(offers.values());
	}
	
	private String normalizeText(String text) {
		String normalizedText = text.replaceAll("\"@.*\\s?"," ").replaceAll("\\r\\n+|\\r+|\\n+", " ").replaceAll("[^A-Za-z0-9\\s]+"," ").replaceAll("[\\s\\t]+", " ").toLowerCase();
		return normalizedText;
	}


	public static String normalizeValue(String rawValue) {
		String normalizedValue = rawValue.replaceAll("\"@.*\\s?"," ").replaceAll("\\s+","").toLowerCase().replaceAll("[^A-Za-z0-9]", "").replaceFirst("^0+(?!$)", "");
		return normalizedValue;
	}

}
