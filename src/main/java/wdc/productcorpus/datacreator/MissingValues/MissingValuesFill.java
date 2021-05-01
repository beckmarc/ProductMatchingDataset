package wdc.productcorpus.datacreator.MissingValues;

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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import ldif.entity.NodeTrait;
import ldif.local.datasources.dump.QuadFileLoader;
import ldif.runtime.Quad;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;
import wdc.productcorpus.util.InputUtil;

/**
 * @author Anna Primpeli
 * Take the descriptive information of the parent node (Product) to fill in information about child nodes (Offers)
 * Consider the relation Product offers Offer
 * This happens when the Offer item is attached an identifier but the name information is found in the product
 * against the google guidelines
 * https://developers.google.com/search/docs/data-types/product
 * where the id information should be found in the product and the offer item gives info about price and availability
 */
public class MissingValuesFill extends Processor<File> {
	
	
	@Parameter(names = { "-in",
	"-inputDir" }, required = true, description = "Folder where the input corpus is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = { "-offers",
	"-offersFile" }, required = true, description = "Folder where the input corpus is read from.", converter = FileConverter.class)
	private File offersFile;
	
	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the output is written to.", converter = FileConverter.class)
	private File outputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	ArrayList<String> textualProperties = new ArrayList<String>() {{
	    add("/brand");
	    add("/name");
	    add("/title");
	    add("/description");

	}};
	
	ArrayList<OutputOffer> offers = new ArrayList<OutputOffer>();
	HashMap<String,Integer> offersIndex = new HashMap<String,Integer>();
	
	@Override
	protected List<File> fillListToProcess() {
		List<File> files = new ArrayList<File>();
		
		if (inputDirectory.isFile()) 
			files.add(inputDirectory);
		else {
			for (File f : inputDirectory.listFiles()) {
				if (!f.isDirectory()) {
					files.add(f);
				}
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
		
		BufferedReader br = InputUtil.getBufferedReader(object);
		
		QuadFileLoader qfl = new QuadFileLoader();
		EntityFileLoader etl = new EntityFileLoader();
		
		String currentURL = "start";
		HashMap<String, List<Quad>> quadsBySubject = new HashMap<String,List<Quad>>();
		
		HashMap<Integer, String> relatedOffersIndex_Prop = new HashMap<Integer,String>();
		HashMap<String, ArrayList<Integer>> relatedOffersIndex_Parent = new HashMap<String,ArrayList<Integer>>();
		int lineCounter =0;
		
		while (br.ready()) {
			lineCounter++;
			if (lineCounter%1000000==0) System.out.printf("Parsed %d lines of file %s",lineCounter, object.getName());
			Quad q = qfl.parseQuadLine(br.readLine());
			
			if (q.graph().equals(currentURL)) {
		
				if (q.value().isResource()) {
					Integer indexOffer = offersIndex.get(q.value().toString()+"_"+q.graph());
							
					if (indexOffer != null) {
						relatedOffersIndex_Prop.put(indexOffer, q.predicate());
						ArrayList<Integer> currentIndices = relatedOffersIndex_Parent.get(q.subject().toString());
						if (currentIndices==null) currentIndices = new ArrayList<Integer>();
						currentIndices.add(indexOffer);
						relatedOffersIndex_Parent.put(q.subject().toString(),currentIndices);
					}
				}
					
				List<Quad> currentQuads = quadsBySubject.get(q.subject().toString());
				if (currentQuads==null) currentQuads=new ArrayList<Quad>();
				currentQuads.add(q);
				quadsBySubject.put(q.subject().toString(), currentQuads);

				
			} else {
				// add parent information to offer from loaded entity in change of url
				if (quadsBySubject.size() > 0) {
					//group quads by subject and create entities
					for (Map.Entry<String, List<Quad>> quadsbyS:quadsBySubject.entrySet()){
						if (relatedOffersIndex_Parent.containsKey(quadsbyS.getKey())) {
							Entity e = etl.loadEntityFromQuads(quadsbyS.getValue());
							
							ArrayList<Integer> relevantOffersToE= relatedOffersIndex_Parent.get(quadsbyS.getKey());
							HashMap<Integer, String> relatedOffers_IndexSUB = new HashMap<Integer, String>();
							for (Integer index: relevantOffersToE)
								relatedOffers_IndexSUB.put(index, relatedOffersIndex_Prop.get(index));
							
							addParentInfo(relatedOffers_IndexSUB, e);	
														
						}
								
						
					}
					relatedOffersIndex_Prop = new HashMap<Integer,String>();
					relatedOffersIndex_Parent = new HashMap<String,ArrayList<Integer>>();
				
				}
				quadsBySubject.clear();
				
				currentURL = q.graph();
				
				List<Quad> currentQuads = quadsBySubject.get(q.subject().toString());
				if (currentQuads==null) currentQuads=new ArrayList<Quad>();
				currentQuads.add(q);
				quadsBySubject.put(q.subject().toString(), currentQuads);
				
				if (q.value().isResource()) {
					Integer indexOffer = offersIndex.get(q.value().toString()+"_"+q.graph());
					
					if (indexOffer != null) {
						relatedOffersIndex_Prop.put(indexOffer, q.predicate());
						ArrayList<Integer> currentIndices = relatedOffersIndex_Parent.get(q.subject().toString());
						if (currentIndices==null) currentIndices = new ArrayList<Integer>();
						currentIndices.add(indexOffer);
						relatedOffersIndex_Parent.put(q.subject().toString(),currentIndices);

					}
				}
			
			}
		}
				
	}
	
	private void addParentInfo(HashMap<Integer, String> relatedOffersIndex, Entity e) {
		
		for (Map.Entry<Integer, String> offerIndex: relatedOffersIndex.entrySet()) {
			OutputOffer offer = offers.get(offerIndex.getKey());
			offer.setParentNodeID(e.getSubject().toString());
			offer.setPropertyToParent(offerIndex.getValue());
			
			for (Map.Entry<String,List<NodeTrait>> prop: e.getProperties().entrySet()){
				for (String keyProp: textualProperties) {
					if (prop.getKey().contains(keyProp)) {
						for (NodeTrait value:prop.getValue()) {
							//found a literal descriptive value
							if (value.toString().startsWith("\"")) {
								HashSet<String> currentValues = offer.getParentdescProperties().get(keyProp);
								if (null == currentValues) currentValues = new HashSet<String>();
								currentValues.add(value.toString());
								offer.getParentdescProperties().put(keyProp, currentValues);
							}
						}
					}
				}
				
			}
			
		}
	}
	
	@Override
	protected void beforeProcess() {
		
		try{
			//load offers
			DataImporter load = new DataImporter(offersFile);
			this.offers = new ArrayList<OutputOffer>(load.importOffers());
			
			
			System.out.printf("Loaded offers %d ",this.offers.size());
			
			System.out.println("Create index for Offers");
			for (int i=0;i<this.offers.size();i++) {
				String key = this.offers.get(i).getNodeID()+"_"+this.offers.get(i).getUrl();
				this.offersIndex.put(key, i);
			}
			
		}
		catch (Exception e){
			System.out.println(e.getMessage());
		}
	}
	
	@Override
	protected void afterProcess() {
		System.out.printf("Write the updated %d offers.",offers.size());
		
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputDirectory));
			for (OutputOffer offer:offers) {
				writer.write(offer.toJSONObject(true)+"\n");
			}
			writer.flush();
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

