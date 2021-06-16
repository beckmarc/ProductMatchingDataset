package wdc.productcorpus.v2.datacreator.filter;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringEscapeUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;

import wdc.productcorpus.datacreator.Extractor.SupervisedNode;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.util.StDevStats;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;
import wdc.productcorpus.v2.util.StringUtils;
import wdc.productcorpus.v2.util.model.LanguageLiteral;
import wdc.productcorpus.v2.util.model.Literal;
import wdc.productcorpus.v2.util.model.LiteralType;

import de.dwslab.dwslib.framework.Processor;

public class ExampleFilter extends Processor<File> {
	
	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	@Parameter(names = "-removeSerial", description = "Removes serial numbers")
	private boolean removeSerial = false;
		
	long validCount = (long)0.0;
	long invalidCount = 0;
		
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
	
	ArrayList<String> examples = new ArrayList<String>(){{
		add("i'm a product");
		//add("i'm a product description");
		add("sono un prodotto");
		add("ich bin ein produkt");
		add("sou um produto"); // portugesisch
		add("soy un producto"); // spanisch
		add("я продукт"); // russian
		add("jag är en produkt");
		add("我是產品");
		add("私は製品です");
		add("ek is 'n produk");
		add("eu sou um produto");
		add("είμαι προϊόν");
		add("ik ben een product");
		add("termék vagyok");
		add("ben bir ürünüm");
		add("sunt un produs");
		add("je suis un produit");
		add("jestem produktem");
		add("olen tuote");
		add("jsem produkt");
		add("аз съм продукт");
		add("sono un prodotto");
		add("ech sinn e produkt");
		add("मैं एक उत्पाद हूँ");
		add("jeg er et produkt");
		add("men mahsulotman");
		add("ฉันเป็นสินค้า");
		add("aš esu produktas");
		add("es esmu produkts");
		add("esto es un producto");
	}};

	@Override
	protected void process(File object) throws Exception {
		
		BufferedReader br = InputUtil.getBufferedReader(object);
		String fileName = CustomFileWriter.removeExt(object.getName());
		
		ArrayList<Entity> entities = new ArrayList<Entity>();
		ArrayList<Entity> validEntities = new ArrayList<Entity>();
		
		String line;
		String currentUrl = "";

		long validCount = 0;
		long invalidCount = 0;

		while ((line = br.readLine()) != null) {	
			Entity e = EntityStatic.parseEntity(line);
			
			if(removeSerial && e.serialNumber != null) { // remove serial Numbers
				e.serialNumber = null; 
				if(!EntityStatic.hasIdentifier(e)) {
					invalidCount++;
					//PrintUtils.p("remove:" + EntityStatic.toJson(e));
					continue;
				}
			}
				
			//PrintUtils.p(decodedName);
			
			if (e.url.equals(currentUrl)) { // the identifier value
			    entities.add(e);
			} else {
				if(entities.size() > 0) {
					ArrayList<Entity> result = processEntities(entities);
					if(result != null && !result.isEmpty()) {
						validEntities.addAll(result);
					}
					else {
						invalidCount += entities.size();
					}
				}
				//write once in a while in file so that we don't keep everything in memory
				if (validEntities.size()>100000) {
					CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "example", validEntities);
					validCount += validEntities.size();
					validEntities.clear();
				}
				entities.clear();
				entities.add(e);
				currentUrl = e.url;
			}
		}
		// process once more for last quads
		if(entities.size() > 0) {
			ArrayList<Entity> result = processEntities(entities);
			if(result != null && !result.isEmpty()) {
				validEntities.addAll(result);
			}
			else {
				invalidCount += entities.size();
			}
		}
		updateCounters(validCount + validEntities.size(), invalidCount);
		CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "example", validEntities);
//		if(negatives)
//		CustomFileWriter.writeEntitiesToFile("negatives", outputDirectory, "", invalidEntities);
		
	}
	
	
		
	/**
	 * Processes entities per URL
	 * 
	 * @param entities
	 * @return
	 */
	public ArrayList<Entity> processEntities(ArrayList<Entity> entities) {
		for(Entity e : entities) {
			if(e.name != null) {
				String decodedName = StringEscapeUtils.unescapeHtml(e.name).toLowerCase();	
				for(String ex : examples) {
					if(StringUtils.levSim(ex, decodedName) > 0.8 || decodedName.contains(ex)) { // either similar or contain 
						return null;
					}
				}
			}
		}
		return entities;
		
	}
	
	
	
	/**
	 * Variationskoeffizient
	 * 
	 * @param nodes
	 * @return
	 */
	private double getVC(ArrayList<Double> counts) {
		StDevStats stats = new StDevStats();
		double[] c = stats.toPrimDouble(counts);
        return stats.getVC(c);
	}
	
	// sync error count with global error count
	private synchronized void updateCounters(long validCount, long invalidCount) {
		this.validCount += validCount;
		this.invalidCount += invalidCount;
	}
	

	@Override
	protected void afterProcess() {
		try {
			System.out.println("Selected entities/lines: " + validCount);
			System.out.println("Deleted entities: " + invalidCount);
			System.out.println("Total entities/lines: " + (validCount + invalidCount));
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
