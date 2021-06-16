package wdc.productcorpus.v2.datacreator.filter;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;
import wdc.productcorpus.datacreator.Extractor.SupervisedNode;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.util.StDevStats;
import wdc.productcorpus.v2.datacreator.ListingsAds.DetectListingsAds;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;

/**
 * @author beckm
 *
 */
public class MainEntityFilter extends Processor<File> {
	@Parameter(names = { "-out",
			"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory;

	@Parameter(names = { "-in",
			"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;

	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;

	

	boolean debug = true;

	public static void main(String args[]) {
		ExampleFilter ddf = new ExampleFilter();
		try {
			new JCommander(ddf, args);
			ddf.process();
		} catch (ParameterException pe) {
			pe.printStackTrace();
			new JCommander(ddf).usage();
		}
	}

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
	
	long deletedCount = (long) 0.0;
	long validEntitiesCount = (long) 0.0;
	long mainEntitiesCount = (long) 0.0;

	@Override
	protected void process(File object) throws Exception {
		String fileName = object.getName().substring(0, object.getName().length() - 4); // removes .txt
		BufferedReader br = InputUtil.getBufferedReader(object);

		ArrayList<Entity> entities = new ArrayList<Entity>();
		ArrayList<Entity> validEntities = new ArrayList<Entity>();
		
		long deletedCount = (long) 0.0;
		long mainEntitiesCount = (long) 0.0;

		String line;
		String currentUrl = "";

		while ((line = br.readLine()) != null) {
			Entity e = EntityStatic.parseEntity(line);

			if (e.url.equals(currentUrl)) { // the identifier value
				entities.add(e);
			} else {
				if (entities.size() > 0) {
					int size = entities.size();
					if(processEntities(entities)) { // true if main entity found
						deletedCount += size - 1;
						mainEntitiesCount++;
					}
					validEntities.addAll(entities);
				}
				// write once in a while in file so that we don't keep everything in memory
				if (validEntities.size() > 50000) {
					CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "main", validEntities);
					updateValidEntitiesCount(validEntities.size());
					validEntities.clear();
				}
				entities.clear();
				entities.add(e);
				currentUrl = e.url;
			}
		}
		// process once more for last quads
		if (entities.size() > 0) {
			int size = entities.size();
			if(processEntities(entities)) { // true if main entity found
				deletedCount += size - 1;
			}
			validEntities.addAll(entities);
		}
		
		updateValidEntitiesCount(validEntities.size());
		updateDeletedCount(deletedCount);
		updateMainEntitiesCount(mainEntitiesCount);
		CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "main", validEntities);
//		if(negatives)
//		CustomFileWriter.writeEntitiesToFile("negatives", outputDirectory, "", invalidEntities);

	}

	// sync error count with global error count
	private synchronized void updateMainEntitiesCount(long count) {
		this.mainEntitiesCount += count;
	}
	
	// sync error count with global error count
	private synchronized void updateValidEntitiesCount(long count) {
		this.validEntitiesCount += count;
	}
	
	// sync error count with global error count
	private synchronized void updateDeletedCount(long count) {
		this.deletedCount += count;
	}

	/**
	 * 
	 * @param entities
	 * @return true if a main product was identified
	 */
	public boolean processEntities(ArrayList<Entity> entities) {
		if (!entities.get(0).isVariation && entities.size() >= 2 && entities.size() < 16 && !DetectListingsAds.checkUrlForListing(entities.get(0).url)) {
			ArrayList<Entity> result = checkForMainProduct(entities);
			if(result.size() == 1) { // main product identified
				entities.clear();
				entities.add(result.get(0));
				return true;
			}
		}
		return false;
	}

	/**
	 * 
	 * @param entities
	 * @param idSim
	 * @return
	 */
	public ArrayList<Entity> checkForMainProduct(ArrayList<Entity> entities) {
		Entity le = entities.get(0); // longest entity description (to determine in next loop)
		ArrayList<Double> lengths = new ArrayList<Double>();
		for (Entity e : entities) {
			if(le.description == null) {
				le = e;
			}
			else if (e.description != null && e.description.length() > le.description.length()) { // get entity with longest description
				le = e;
			}
		}

		for (Entity e : entities) {
			if(e.description != null && !e.equals(le)) { // check everything except longest entity
				lengths.add((double) e.description.length());
			}
		}
		
		
		
		if (le.description != null && (le.description.length() / mean(lengths)) > 2.5) {
//			PrintUtils.p("Longest Desc to mean Desc Length koeffizient:" + (le.description.length() / mean(lengths)));
//			PrintUtils.p(EntityStatic.toJson(le));
//			PrintUtils.p("\n");
//			PrintUtils.p(EntityStatic.toJson(entities));
//			try {
//				entities.add(le);
//				CustomFileWriter.writeEntitiesToFile("deleted", outputDirectory, "", entities);
//			} catch (IOException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}
			ArrayList<Entity> result = new ArrayList<Entity>();
			result.add(le);
			return result;
		}
		else {
			return entities;
		}
	}

	private double mean(ArrayList<Double> lengths) {
		StDevStats st = new StDevStats();
		return st.getMean(st.toPrimDouble(lengths));
	}

	private double calcIdentifierSimilarity(ArrayList<Entity> entities) {
		ArrayList<String> gtin12s = new ArrayList<String>();
		ArrayList<String> gtin13s = new ArrayList<String>();
		ArrayList<String> gtin14s = new ArrayList<String>();
		ArrayList<String> gtin8s = new ArrayList<String>();
		ArrayList<String> mpns = new ArrayList<String>();
		ArrayList<String> productIds = new ArrayList<String>();
		ArrayList<String> skus = new ArrayList<String>();
		ArrayList<String> identifiers = new ArrayList<String>();
		ArrayList<String> gtin = new ArrayList<String>();
		ArrayList<String> serialNumber = new ArrayList<String>();

		for (Entity e : entities) {
			if (e.gtin12 != null)
				gtin12s.add(e.gtin12);
			if (e.gtin13 != null)
				gtin13s.add(e.gtin13);
			if (e.gtin14 != null)
				gtin13s.add(e.gtin14);
			if (e.gtin8 != null)
				gtin8s.add(e.gtin8);
			if (e.mpn != null)
				mpns.add(e.mpn);
			if (e.productID != null)
				productIds.add(e.productID);
			if (e.sku != null)
				skus.add(e.sku);
			if (e.identifier != null)
				identifiers.add(e.identifier);
			if (e.serialNumber != null)
				serialNumber.add(e.serialNumber);
			if (e.gtin != null)
				gtin.add(e.gtin);

		}

		double identifierSimilarity = (+length(gtin12s) * calcSim(gtin12s) + length(gtin13s) * calcSim(gtin13s)
				+ length(gtin14s) * calcSim(gtin14s) + length(gtin8s) * calcSim(gtin8s) + length(mpns) * calcSim(mpns)
				+ length(skus) * calcSim(skus) + length(identifiers) * calcSim(identifiers)
				+ length(serialNumber) * calcSim(serialNumber) + length(gtin) * calcSim(gtin)
				+ length(productIds) * calcSim(productIds))
				/ (+length(gtin12s) + length(gtin13s) + length(gtin14s) + length(gtin8s) + length(gtin)
						+ length(serialNumber) + length(mpns) + length(skus) + length(identifiers)
						+ length(productIds));
		return identifierSimilarity;
	}

	private double calcTextualSimilarity(ArrayList<Entity> entities) {
		ArrayList<String> names = new ArrayList<String>();
		ArrayList<String> descriptions = new ArrayList<String>();
		ArrayList<String> brands = new ArrayList<String>();

		for (Entity e : entities) {
			if (e.name != null) {
				names.add(e.name);
			}
			if (e.description != null) {
				descriptions.add(e.description);
			}
			if (e.brand != null) {
				brands.add(e.brand);
			}
		}

		double textSimilarity = (+length(names) * calcSim(names) + length(descriptions) * calcSim(descriptions)
				+ length(brands) * calcSim(brands)) / (+length(names) + length(descriptions) + length(brands));
		return textSimilarity;
	}

	/**
	 * Uses simple calc
	 * 
	 * @param entities
	 * @return
	 */
	private double calcNameSimilarity(ArrayList<Entity> entities) {
		ArrayList<String> names = new ArrayList<String>();
		for (Entity e : entities) {
			if (e.name != null) {
				names.add(e.name);
			}
		}
		double nameSimilarity = (calcSim(names));
		return nameSimilarity;
	}

	/**
	 * Uses simple calc
	 * 
	 * @param entities
	 * @return
	 */
	private double calcDescriptionSimilarity(ArrayList<Entity> entities) {
		ArrayList<String> descriptions = new ArrayList<String>();
		for (Entity e : entities) {
			if (e.description != null) {
				descriptions.add(e.description);
			}
		}
		double descriptionsSimilarity = (calcSim(descriptions));
		return descriptionsSimilarity;
	}

	private double length(ArrayList<String> list) {
		double size = 0.0;
		for (String s : list) {
			size += s.length();
		}
		// PrintUtils.p(size);
		return size;
	}

	/**
	 * Computes the similarity from the first entry with every other entry
	 * 
	 * @param list
	 * @return
	 */
	private double calcSim(ArrayList<String> list) {
		NormalizedLevenshtein nl = new NormalizedLevenshtein();
		ArrayList<Double> distances = new ArrayList<Double>();
		for (int i = 1; i < list.size(); i++) {
			double sim = 1.0 - nl.distance(list.get(0).toLowerCase(), list.get(i).toLowerCase());
			distances.add(sim);
		}
		return distances.stream().mapToDouble(d -> d).average().orElse(0.0);
	}



	/**
	 * Calculates Standard Deviation of the length of the textual descriptions
	 * 
	 * @param nodes
	 * @return
	 */
	private double getCV(ArrayList<Double> counts) {
		StDevStats stats = new StDevStats();
		double[] c = stats.toPrimDouble(counts);
		return stats.getVC(c);
	}

	@Override
	protected void afterProcess() {
		try {
			System.out.println("Main Entities found: " + mainEntitiesCount);
			System.out.println("Deleted Entities: " + deletedCount);
			System.out.println("Selected Entities: " + validEntitiesCount);
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
