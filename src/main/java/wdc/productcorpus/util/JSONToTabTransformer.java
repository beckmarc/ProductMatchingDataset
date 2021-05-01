package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.json.JSONObject;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;


/**
 * @author Anna Primpeli
 * Transform for clustering to tab separated input. node\turl\tidprop\tidvalue
 */
public class JSONToTabTransformer extends Processor<File> {

	File inputDir = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\input");

	File outputDir = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp");
	int threads =1;
	String filter= "gtin";
	
	public static void main (String args[]) throws IOException {
		
		JSONToTabTransformer convert = new JSONToTabTransformer();
		
		if (args.length>0) {
			
			convert.inputDir = new File(args[0]);
			convert.outputDir = new File (args[1]);
			convert.threads = Integer.parseInt(args[2]);
			convert.filter = args[3];
		}
		
		convert.process();
				
	}
	
	@Override
	protected List<File> fillListToProcess() {
		List<File> files = new ArrayList<File>();
		for (File f : inputDir.listFiles()) {
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
	
	public void process (File object) throws IOException {
		
		
		BufferedReader reader = InputUtil.getBufferedReader(object);
		DataImporter importOffer = new DataImporter();
		String line="";

		BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir+"/"+object.getName()+"_tab"));
		int offersWithManyIDs = 0;
		
		HashSet<String> linesWManyIdentifiers= new HashSet<String>();
		while ((line=reader.readLine())!=null) {
			
			JSONObject json = new JSONObject(line);
			
			OutputOffer offer = importOffer.jsonToOffer(json);
			
			for (Map.Entry<String, HashSet<String>> idvalues: offer.getIdentifiers().entrySet()) {
				
				if (idvalues.getValue().size()>3) {
					linesWManyIdentifiers.add(line);
					offersWithManyIDs++;
					continue;
				}
				
				
				if (!filter.equals("no") && !idvalues.getKey().contains(filter)) continue;

				for (String value:idvalues.getValue()) {
					writer.write(object.getName()+"\t"+offer.getNodeID()+"\t"+offer.getUrl()+"\t"+idvalues.getKey()+"\t"+value.replaceAll(" ","")+"\n");
				}
				
				//keep one value - consistent with the previous creation of the corpus
				if (idvalues.getValue().size()>1) {
					continue;
				}
				
			}
			//write as tab format one line for every identifier
			
		}
		
		System.out.println("Lines with many identifiers");
		for (String l:linesWManyIdentifiers) {
			System.out.println(l);
		}
		
		writer.flush();
		writer.close();
		
		System.out.println("Offer-id prop with many id values: "+offersWithManyIDs);
	}


}
