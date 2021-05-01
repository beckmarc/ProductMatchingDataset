package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;

/**
 * @author Anna Primpeli
 * Removes duplicate offers. Duplicate offers appear because our extractor might assign different nodeIDs to the same entity.
 * We recognize every unique entity by its url-identifier-identifiervalue-text.
 * Duplicate offers may appear across files
 */
public class DeduplicatorSerial {

	@Parameter(names = { "-out",
	"-outputDir" }, required = false, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp"); 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = false, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\offers_clean.txt");
	
	public static void main(String args[]) throws IOException {
		DeduplicatorSerial dedup = new DeduplicatorSerial();
		if (args.length>0) {
			dedup.inputDirectory=new File(args[0]);
			dedup.outputDirectory=new File(args[1]);
		}
		dedup.deduplicate();
	}
	
	
	long filteredLines = (long)0.0;

	HashSet<String> keys = new HashSet<String>();
	
	public void deduplicate() throws IOException {
		
		//for (File f : inputDirectory.listFiles()) {
			if (!inputDirectory.isDirectory()) {
				
				ArrayList<String> deduplicatedData = new ArrayList<String>();
				BufferedReader br = InputUtil.getBufferedReader(inputDirectory);
				String line;
				DataImporter importOffer = new DataImporter();

				while ((line = br.readLine()) != null) {
					
					JSONObject json = new JSONObject(line);
					
					OutputOffer offer = importOffer.jsonToOffer(json);
					
					//String lineParts[] = line.split("\\t");
					//keys are pld, identifier value and text
					//String domain =  DomainUtil.getPayLevelDomainFromWholeURL(lineParts[2]);
					//String key = lineParts[2]+""+""+lineParts[3]+""+lineParts[4]+""+lineParts[5];
					String key = offer.getUrl()+offer.getIdentifierPropertiesAsOneString()+offer.getDescriptivePropertiesAsOneString();
					System.out.println(key);
					if (keys.contains(key)) {
						filteredLines++;
						continue;
					}
					else {
						deduplicatedData.add(line);
						keys.add(key);
					}
					if (deduplicatedData.size()%1000000==0) {
						writeInFile(inputDirectory.getName(), deduplicatedData);

						deduplicatedData=new ArrayList<String>();
					}
				}
				
				//write last part
				writeInFile(inputDirectory.getName(), deduplicatedData);

			}
		//}
		afterProcess();
	}
	

	private void writeInFile(String fileName, ArrayList<String> data) throws IOException {
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/"+fileName+"_DEDUP.json",true));
		
		for (String line:data)
			writer.write(line+"\n");
		
		writer.flush();
		writer.close();
		
	}
	
	private void afterProcess() {
		System.out.println("Filtered lines because of duplicated pld-idproperty-idvalue-text: "+filteredLines);
		System.out.println("Deduplicated output saved in: "+outputDirectory);
		System.out.println("DONE");
	}
}
