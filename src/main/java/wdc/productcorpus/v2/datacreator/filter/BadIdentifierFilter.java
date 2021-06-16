package wdc.productcorpus.v2.datacreator.filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.uri.DomainUtil;
import wdc.productcorpus.datacreator.Extractor.EnhancedIdentifierExtractor;
import wdc.productcorpus.datacreator.Extractor.SupervisedNode;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;
import wdc.productcorpus.v2.util.StringUtils;
import wdc.productcorpus.v2.util.model.LanguageLiteral;
import wdc.productcorpus.v2.util.model.Literal;
import wdc.productcorpus.v2.util.model.LiteralType;

/**
 * Normalizes the identifier values and filters out offers with bad identifiers. These can be:
 * 
 * - to short identifier (length < 8)
 * - to long identifiers (length > 25)
 * 
 * 
 * Normalizing steps:
 * - removing common faulty prefixes like sku and ean and spaces
 * 
 * @author beckm
 *
 */
public class BadIdentifierFilter extends Processor<File> { 
		
	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	long eliminatedLines = (long) 0.0;
	long selectedLines = (long) 0.0;
	long prefixes = (long) 0.0;
	
	private int minimumNumberofClassTokens = 8;
	private int maximumNumberofClassTokens = 25;
	
	boolean debug = true;
	
	public static void main(String args[]) {
		PrintUtils.p(customLength("се-ые/25шт./100гри"));
		
	}
	
	
	private ArrayList<String> commonPrefixes = new ArrayList<String>(){{
	    add("sku");
	    add("id");
	    add("item");
	    add("isbn");
	    add("ean");
	    add("upc");
	    add("https");
	    add("stock");
	    add("part");
	    add("style");
	    add("upc");
	    add("product");
	    add("mpn");
	    add("gtin");
	    add("serialNumber");
	    add("number");
	    add("nr");
	    add("http");
	}};
	
	private ArrayList<String> postPrefixes = new ArrayList<String>(){{
	    add(":");
	    add(";");
	}};
	
	
	
	
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
		String fileName = object.getName().substring(0, object.getName().length()-4); // removes .txt
		BufferedReader br = InputUtil.getBufferedReader(object);
		String line;
		ArrayList<Entity> selectedEntities = new ArrayList<Entity>();
		int eliminatedLines = 0;
		int entitiesCount = 0;

		while ((line = br.readLine()) != null) { 
			Entity entity = EntityStatic.parseEntity(line);
			if(entity != null) {
				normalizeIdentifiers(entity);
				if(containsValidIdentifiers(entity)) {
					selectedEntities.add(entity);
				} else {
					eliminatedLines++;
				}
			}
			//write once in a while in file so that we don't keep everything in memory
			if (selectedEntities.size()>100000) {
				CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "idFilter", selectedEntities);
				entitiesCount += selectedEntities.size();
				selectedEntities.clear();
			}
		}
		
		// Save Statistics
		integrateEliminatedLines(eliminatedLines);
		integrateSelectedLines(entitiesCount + selectedEntities.size());
		
		//write the last part		
		CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "idFilter", selectedEntities);
	}
	
	/**
	 * Normalize a node. This means the following: <br>
	 * - remove prefixes contained in the id's values
	 * 
	 * @param node
	 * @return
	 */
	private void normalizeIdentifiers(Entity e) {
		if(e.gtin12 != null)
			e.gtin12 = normalize(e.gtin12, e);
		if(e.gtin13 != null)
			e.gtin13 = normalize(e.gtin13, e); 
		if(e.gtin14 != null)
			e.gtin14 = normalize(e.gtin14, e); 
		if(e.gtin8 != null)
			e.gtin8 = normalize(e.gtin8, e);
		if(e.mpn != null)
			e.mpn = normalize(e.mpn, e); 
		if(e.productID != null)
			e.productID = normalize(e.productID, e); 
		if(e.sku != null)
			e.sku = normalize(e.sku, e); 
		if(e.identifier != null)
			e.identifier = normalize(e.identifier, e); 	
		if(e.gtin != null)
			e.gtin = normalize(e.gtin, e); 
		if(e.serialNumber != null)
			e.serialNumber = normalize(e.serialNumber, e); 
	}
	
	/**
	 * Normalizes a given id value. Applies two types of operations:<br>
	 * 1.) Mutates string to a "normal" form<br>
	 * 2.) If the string is invalid set it to null (e.g. to short or to long identifiers)
	 * 
	 * @param id
	 * @return
	 */
	private String normalize(String id, Entity e) {
		// Normalizes String
		id = id.replaceAll("(\\s{2,})|(\\\\n)|(\\\\r)|(\\\\t)|(\ufffd)|(\\|)|(\\&)|\\^", "").trim();	
		id = removePrefixes(id);
		// Checks the string (this is the validation)
		if(customLength(id) < this.minimumNumberofClassTokens 
				|| customLength(id) > this.maximumNumberofClassTokens || isTextual(id) || isSimilar(id, e) || isMalformed(id) ) {
			id = null;
		}
		return id;
	}
	
	/**
	 * Method for smaller adjustments
	 * @param id
	 * @return
	 */
	private boolean isMalformed(String id) {
		return StringUtils.containsOnlyZeros(id);
	}
	
	/**
	 * Identifier should not be similar to the name and desc 
	 * (allthough rules are not so strict for this implementation because of eg mainboard names:
	 * 756TXDCB02K062)
	 * @param id
	 * @param e
	 * @return
	 */
	private boolean isSimilar(String id, Entity e) {
		if(e.name != null && StringUtils.levSim(id, e.name) > 0.9 && containsSpaces(id)) {
			//PrintUtils.p("to similar:" + id +"  "+ e.name);
			return true;
		}
		else if(e.description != null && StringUtils.levSim(id, e.description) > 0.9 && containsSpaces(id)) {
			//PrintUtils.p("to similar" + id +" desc "+ e.description);
			return true;
		}
		else {
			return false;
		}
	}
	
	private boolean containsSpaces(String s) {
		return s.length() - s.replaceAll(" ", "").length() > 0;
	}
	
	/**
	 * Only count letters and numbers
	 * @param id
	 * @return
	 */
	private static int customLength(String id) {
		return id.replaceAll("/|\\.|-|\\)|\\(|\\s|_|:|,|\\^", "").length();
	}

	/**
	 * Checks if the identifier value is valid
	 * 
	 * @param node
	 * @return
	 */
	private boolean containsValidIdentifiers(Entity e)  {	
		return e.gtin12 != null || e.gtin13 != null || e.gtin14 != null || e.gtin8 != null 
				|| e.mpn != null || e.productID != null || e.sku != null || e.identifier != null 
					|| e.gtin != null || e.serialNumber != null;
	}
	

	/**
	 * Checks if an identifier is in a more textual representation
	 * e.g. s-5 white
	 * 
	 * @param value
	 * @return
	 */
	private boolean isTextual(String value) {
		boolean isTextual = false;
		char[] chars = value.toCharArray();
		int charCount = 0;
	    for (char c : chars) {
	        if(Character.isLetter(c)) {
	            charCount++;
	        }
	    }
	    if(customLength(value) == charCount) {
	    	isTextual = true;
	    }
		return isTextual;
	}
	
	/**
	 * Removes prefixes in the identifiers values like sku: or gtin
	 * @param value
	 * @return
	 */
	private String removePrefixes(String value) {
		String normValue = value;
		for (String prefix:this.commonPrefixes) {
			// first check all prefixes + postprefixes e.g. "sku" + ":"
			for(String postPrefix:this.postPrefixes) {  
				if (value.toLowerCase().startsWith(prefix + postPrefix)) {
					normValue = normValue.replaceFirst(prefix + postPrefix, "");
					integratePrefixes(1);
				}
			}
			// then check for each normal prefix
			if (value.toLowerCase().startsWith(prefix)) {
				normValue = normValue.toLowerCase().replaceFirst(prefix, "");
				integratePrefixes(1);
			}
		}
		return normValue;
	}
	
	private LiteralType getIdLiteral(SupervisedNode node) {
		if(LanguageLiteral.isType(node.getNormalizedValue())) {
			return new LanguageLiteral(node.getNormalizedValue());
		} 
		else {
			return new Literal(node.getNormalizedValue());
		}
	}

	private synchronized void integrateEliminatedLines (Integer lines) {
		this.eliminatedLines += lines;
	}
	
	private synchronized void integrateSelectedLines (Integer lines) {
		this.selectedLines += lines;
	}
	
	private synchronized void integratePrefixes (Integer lines) {
		this.prefixes += lines;
	}


	@Override
	protected void afterProcess() {
		try {
			System.out.println("Selected lines: " + selectedLines);
			System.out.println("Eliminated lines: " + eliminatedLines);
			System.out.println("Prefixes found and deleted in Ids: " + prefixes);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
