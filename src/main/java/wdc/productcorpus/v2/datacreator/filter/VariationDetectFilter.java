package wdc.productcorpus.v2.datacreator.filter;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
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
public class VariationDetectFilter extends Processor<File> {
	@Parameter(names = { "-out",
			"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory;

	@Parameter(names = { "-in",
			"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;

	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;

	long duplicateDescriptionLines = (long) 0.0;
	long emptyDescriptionLines = (long) 0.0;
	long selectedLines = (long) 0.0;

	long variationEntitiesCount = (long) 0.0;

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
	
	

	@Override
	protected void process(File object) throws Exception {
		String fileName = object.getName().substring(0, object.getName().length() - 4); // removes .txt
		BufferedReader br = InputUtil.getBufferedReader(object);

		ArrayList<Entity> entities = new ArrayList<Entity>();
		ArrayList<Entity> validEntities = new ArrayList<Entity>();
		
		long variationEntitiesCount = (long) 0.0;

		String line;
		String currentUrl = "";

		while ((line = br.readLine()) != null) {
			Entity e = EntityStatic.parseEntity(line);

			if (e.url.equals(currentUrl)) { // the identifier value
				entities.add(e);
			} else {
				if (entities.size() > 0) {
					if(processEntities(entities)) {
						CustomFileWriter.writeEntitiesToFile("variations", outputDirectory, "", entities);
						variationEntitiesCount += entities.size();
					}
					validEntities.addAll(entities);
				}
				// write once in a while in file so that we don't keep everything in memory
				if (validEntities.size() > 100000) { 
					CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "variation", validEntities);
					validEntities.clear();
				}
				entities.clear();
				entities.add(e);
				currentUrl = e.url;
			}
		}
		// process once more for last quads
		if (entities.size() > 0) {
			if(processEntities(entities)) {
				variationEntitiesCount += entities.size();
			}
			validEntities.addAll(entities);
		}
		
		updateVariationEntitiesCount(variationEntitiesCount);
		CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "variation", validEntities);
//		if(negatives)
//		CustomFileWriter.writeEntitiesToFile("negatives", outputDirectory, "", invalidEntities);

	}

	// sync error count with global error count
	private synchronized void updateVariationEntitiesCount(long count) {
		this.variationEntitiesCount += count;
	}

	/**
	 * 
	 * @param entities
	 * @return true if entities could represent variations
	 */
	public boolean processEntities(ArrayList<Entity> entities) {
		if (!entities.get(0).isVariation && entities.size() >= 3 &&
				!DetectListingsAds.checkUrlForListing(entities.get(0).url) && isVariation(entities)) {		
			for (Entity e : entities) {
				e.isVariation = true;
			}
			//PrintUtils.p(EntityStatic.toJson(entities));
			return true;
		}
		return false;
	}

	public boolean isVariation(ArrayList<Entity> entities) {
		double textSim = calcTextualSimilarity(entities);
		double idSim = calcIdentifierSimilarity(entities);
		PrintUtils.p("textSim:" + textSim);
		PrintUtils.p("idSim:" + idSim);
		PrintUtils.p(EntityStatic.toJson(entities));
		if (entities.size() < 20 && (textSim + idSim) > 1.5) {
			
			return true;
		}
		return false;
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

	public static double calcTextualSimilarity(ArrayList<Entity> entities) {
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
		
		//PrintUtils.p(calcSim(names) +  "  "  + calcSim(descriptions));

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

	private static double length(ArrayList<String> list) {
		double size = 0.0;
		for (String s : list) {
			size += s.length();
		}
		// PrintUtils.p(size);
		return size;
	}

	/**
	 * Calculates the similarity over all similarities in the arraylist<br>
	 * results in quadratic complexity
	 * 
	 * @param list
	 * @return
	 */
	private double calcAvgSim(ArrayList<String> list) {
		NormalizedLevenshtein nl = new NormalizedLevenshtein();
		ArrayList<Double> distances = new ArrayList<Double>();
		for (int i = 0; i < list.size(); i++) {
			for (int j = i + 1; j < list.size(); j++) {
				double sim = 1.0 - nl.distance(list.get(i).toLowerCase(), list.get(j).toLowerCase());
				distances.add(sim);
			}
		}
		return distances.stream().mapToDouble(d -> d).average().orElse(0.0);
	}

	/**
	 * Computes the similarity from the first entry with every other entry
	 * and from the last with every other entity. Picks the higher value then.
	 * 
	 * @param list
	 * @return
	 */
	private static double calcSim(ArrayList<String> list) {
		NormalizedLevenshtein nl = new NormalizedLevenshtein();
		ArrayList<Double> distances = new ArrayList<Double>();
		for (int i = 1; i < list.size(); i++) { // from first element
			double sim = 1.0 - nl.distance(list.get(0).toLowerCase(), list.get(i).toLowerCase());
			distances.add(sim);
		}
		double v1 = distances.stream().mapToDouble(d -> d).average().orElse(0.0);
		distances.clear();
		Collections.reverse(list);
		for (int i = 1; i < list.size(); i++) { // from first element
			double sim = 1.0 - nl.distance(list.get(0).toLowerCase(), list.get(i).toLowerCase());
			distances.add(sim);
		}
		double v2 = distances.stream().mapToDouble(d -> d).average().orElse(0.0);
		return v1 >= v2 ? v1 : v2; // always return the higher similarity
	}

	/**
	 * Calculates Standard Deviation of the length of the textual descriptions
	 * 
	 * @param nodes
	 * @return
	 */
	private boolean stdDev(ArrayList<Entity> nodes) {
		return false;

	}

	private Entity mergeEntities(HashSet<Entity> entity) {
		return null;
	}

	private synchronized void integrateEmptyDescriptionLines(Integer lines) {
		this.emptyDescriptionLines += lines;
	}

	private synchronized void integrateDuplicateDescriptionLines(Integer lines) {
		this.duplicateDescriptionLines += lines;
	}

	private synchronized void integrateSelectedLines(Integer lines) {
		this.selectedLines += lines;
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
			System.out.println("Marked variations: " + variationEntitiesCount);
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
