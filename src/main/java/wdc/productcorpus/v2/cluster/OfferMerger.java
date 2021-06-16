package wdc.productcorpus.v2.cluster;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;


public class OfferMerger extends Processor<File> {

	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-inOffer" }, description = "File that contains the offer mappings to clusters", converter = FileConverter.class)
	private File inputOffersDir;
	
	@Parameter(names = { "-inCorpus" }, required = true, description = "Folder where the input files are read from.", converter = FileConverter.class)
	private File inputCorpusDir;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	private HashMap<String, Entity> mappedEntities = new HashMap<String, Entity>();
	
	
	@Override
	protected List<File> fillListToProcess() {
		List<File> files = new ArrayList<File>();
		if (inputCorpusDir.isDirectory()) {
			for (File f : inputCorpusDir.listFiles()) {
				if (!f.isDirectory()) {
					files.add(f);
				}
			}
		}
		return files;
	}
	
	/**
	 * Reads the file that contains only the mapping from url + nodeid to cluster_id
	 */
	@Override
	protected void beforeProcess() {
		String line;
		try {
			for(File f : inputOffersDir.listFiles()) {
				if (!f.isDirectory() && f.getName().equals("offers.json")) { 
					BufferedReader br = InputUtil.getBufferedReader(f);
					while ((line = br.readLine()) != null) {
						Entity e = EntityStatic.parseEntity(line);
						mappedEntities.put(getKey(e), e);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	protected int getNumberOfThreads() {
		return this.threads;
	}
	
	@Override
	protected void process(File object) throws Exception {	
		String fileName = CustomFileWriter.removeExt(object.getName());
		BufferedReader br = InputUtil.getBufferedReader(object);
		ArrayList<Entity> entities = new ArrayList<Entity>();
		String line;
		while ((line = br.readLine()) != null) {	
			Entity e = EntityStatic.parseEntity(line); // entity from the big corpus with all attributes
			if(mappedEntities.get(getKey(e)) != null) {
				e.cluster_id = mappedEntities.get(getKey(e)).cluster_id;
				entities.add(e);
			}
			
			if (entities.size()>100000) {
				CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "cluster", entities);
				entities.clear();
			}
		}
		CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "cluster", entities);
	}
	
	
	private String getKey(Entity e) {
		return e.nodeId + "\t" + e.url;
	}
	
	@Override
	protected void afterProcess() {
		try {
			System.out.println("Finished");
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	
}
