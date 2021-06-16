package wdc.productcorpus.v2.datacreator.filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.util.DomainUtil;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;

public class PLDFilterEntitiesEnhanced extends Processor<File>{
	
	private File outputDirectory; 
	private File inputDirectory;
	private Integer threads;
	HashMap<String, HashSet<String>> badplds = new HashMap<String, HashSet<String>>();
	
	public PLDFilterEntitiesEnhanced(File output, File input, Integer threads, HashMap<String, HashSet<String>> badplds) {
		this.outputDirectory = output;
		this.inputDirectory = input;
		this.threads = threads; 
		this.badplds = badplds;
	}
	
	
	
	long eliminatedLines = (long)0.0;
	long selectedLines = (long)0.0;

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

	@Override
	protected void process(File object) throws Exception {
		BufferedReader br = InputUtil.getBufferedReader(object);
		String fileName = CustomFileWriter.removeExt(object.getName());
		
		ArrayList<Entity> filteredData = new ArrayList<Entity>();
		String line;
		long eliminatedLines =(long) 0.0;
		long selectedLines =(long) 0.0;

		while ((line = br.readLine()) != null) {
			
			Entity e = EntityStatic.parseEntity(line);
			String domain = DomainUtil.getPayLevelDomainFromWholeURL(e.url);
			
			// remove identifiers from entities
			if (null != domain && badplds.keySet().contains(domain)) {
				for(String idValue : badplds.get(domain)) {
					EntityStatic.removeIdentifierByValue(idValue, e);
				}
			}
			
			// only add if it has identifier
			if(EntityStatic.hasIdentifier(e)) {
				filteredData.add(e);
				selectedLines++;
			} else {
				eliminatedLines++;
			}
				
			
			if (filteredData.size()>50000) {
				CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "pldEnhanced", filteredData);
				filteredData.clear();
			}
		}
		
		
		//write the last part
		CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "pldEnhanced", filteredData);
		filteredData.clear();
		integrateElimLines(eliminatedLines);
		integrateSelectedLines(selectedLines);
	}
	
	private synchronized void integrateElimLines(long eliminatedLines) {
		this.eliminatedLines += eliminatedLines;
	}
	
	private synchronized void integrateSelectedLines(long eliminatedLines) {
		this.selectedLines += eliminatedLines;
		
	}

	
	@Override
	protected void afterProcess() {
		try {
			System.out.println("Selected Offers: "+selectedLines);
			System.out.println("Eliminated Offers: "+eliminatedLines);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
