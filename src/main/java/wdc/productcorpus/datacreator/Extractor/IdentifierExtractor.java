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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Edge;
import ldif.local.datasources.dump.QuadFileLoader;
import ldif.runtime.Quad;
import scala.actors.threadpool.Arrays;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;
import wdc.productcorpus.v2.util.QuadParser;
import wdc.productcorpus.v2.util.StringUtils;

/**
 
 *
 */
@Parameters(commandDescription = "")
public class IdentifierExtractor extends Processor<File> {

	@Parameter(names = { "-out",
			"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 

	@Parameter(names = { "-in",
			"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;

	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;

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
	
	ArrayList<String> parentChildProperties = new ArrayList<String>() {{
	    add("/offers");
	}};
	
	/*
	ArrayList<String> identifiers = new ArrayList<String>() {{
	    add("/telephone");
	    add("/duns");
	}};
	
	ArrayList<String> textualProperties = new ArrayList<String>() {{
	    add("/servesCuisine");
	    add("/priceRange");
	    add("/address");
	    add("/alumni");
	    add("/legalName");
	    add("/latitude");
	    add("/longitude");
	    add("/description");
	    add("/name");


	}};*/
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

	private long errorCount = 0;
	private long parsedLines = 0;
	private long noDesc = 0;
	private long desc = 0;

	@Override
	protected void process(File object) throws Exception {
				
		long lineCount = (long)0.0;
		int errorCount = 0;

		QuadParser qp = new QuadParser(true);
		String currentId = "";
		List<Quad> quads = new ArrayList<Quad>();
		// read the file
		BufferedReader br = InputUtil.getBufferedReader(object);
		
		ArrayList<Entity> nodesWithSupervision = new ArrayList<Entity>();
		
		int nodesWithSupervisionCount = 0;
		Quad eq = null;
		String currentLine = "";
		ArrayList<String> errorLines = new ArrayList<String>();
		
		while (br.ready()) {
			try {
				//Quad q = qfl.parseQuadLine(br.readLine());
				currentLine = br.readLine();
				Quad q = qp.parseQuadLine(currentLine);
				lineCount++;
				
				if(lineCount % 1000000 == 0) {
					System.out.println("Parsed "+lineCount+" from file:"+object.getName());
				}
				
				if (q.subject().value().equals(currentId)) {
					quads.add(q);
				} else {
					if (quads.size() > 0) {
						Entity entity = parseEntity(quads, object.getName());
						if(entity != null) {
							nodesWithSupervision.add(entity);
						}
						
						//write once in a while in file so that we don't keep everything in memory
						if (nodesWithSupervision.size()>100000) {
							CustomFileWriter.writeEntitiesToFile(object.getName(), outputDirectory, "extract", nodesWithSupervision);
							nodesWithSupervisionCount += nodesWithSupervision.size();
							nodesWithSupervision.clear();
						}
					}
					quads.clear();
					quads.add(q);
					currentId = q.subject().value();
				}
			} catch (Exception e) {
				e.printStackTrace();
//				System.out.println(currentLine);
//				System.out.println(eq);
				errorLines.add(currentLine);
				errorCount++;
			}
		}
		// process once more for the last quads
		if (quads.size() > 0) {
			Entity entity = parseEntity(quads, object.getName());
			if(entity != null) {
				nodesWithSupervision.add(entity);
			}
		}
		br.close();
		nodesWithSupervisionCount += nodesWithSupervision.size();

		System.out.println("Entities with supervision: "+nodesWithSupervisionCount+" in file:"+object.getName());
		updateErrorCount(errorCount);
		updateLineCount(lineCount);
		String fileName = CustomFileWriter.removeExt(object.getName());
		CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "extract", nodesWithSupervision);
		// writeErrorsInFile("errors.txt", errorLines);
	}

	private Entity parseEntity(List<Quad> quads, String fileName) {	
		Entity entity = new Entity();
		entity.hasId = false;
		
		for (Quad q: quads){ 
			
			for (String id:identifiers){
				if (q.predicate().contains(id) && !q.value().value().trim().equals("")) {
					entity.hasId = true;
					EntityStatic.setIdentifyingProperty(entity, id, q.value().value());			
				}
			}
			
			for (String textProp:textualProperties) {
				//get the text of literal of object of the predicates we have defined			
				if (q.predicate().contains(textProp) && q.value().toString().startsWith("\"") ) {			
					EntityStatic.setTextualProperty(entity, textProp, normalizeText(q.value().value()));	
				}
			}
			
		}
		
		//if we found an interesting entity store it
		if(entity.hasId) {		
			entity.fileId = fileName;
			entity.url = quads.get(0).graph(); // all quads have the same graph label
			entity.nodeId = quads.get(0).subject().toString();
			return entity;
		}
		else {
			return null;
		}
	}
	
	/**
	 * Normalizes textual properties. Returns null if the string is empty after the process.
	 * @param text
	 * @return
	 */
	public String normalizeText(String text) {
		String normalizedText = text.replaceAll("(\\s{2,})|(\\\\n)|(\\\\r)|(\\\\t)", ""); 
		normalizedText = normalizedText.replaceAll("\u00A0", "").trim();
		if(normalizedText.equals("")) {
			return null;
		}
		return normalizedText;
	}
	
	public static String normalizeValue(String rawValue) {
		String normalizedValue = rawValue.toLowerCase();
		return normalizedValue;
	}

	// sync error count with global error count
	private synchronized void updateErrorCount(int errorCount) {
		this.errorCount += errorCount;
	}

	// sync line count with global line count
	private synchronized void updateLineCount(long lineCount) {
		this.parsedLines += lineCount;
	}
	
	// sync line count with global line count
	private synchronized void updateNoDescCount(long lineCount) {
		this.noDesc  += lineCount;
	}
	
	// sync line count with global line count
	private synchronized void updateDescCount(long lineCount) {
		this.desc  += lineCount;
	}

	

	@Override
	protected void afterProcess() {
		try {
			System.out.println("Parsed " + parsedLines + " lines.");
			System.out.println("Could not parse " + errorCount + " lines (quads).");
			System.out.println("Found entities with no textual description: " + noDesc);
			System.out.println("Amount of entities where text was added: " + desc);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	

	public static void main(String[] args) {
//		IdentifierExtractor cal = new IdentifierExtractor();
//		try {
//			new JCommander(cal, args);
//			cal.process();
//		} catch (ParameterException pe) {
//			pe.printStackTrace();
//			new JCommander(cal).usage();
//		}
	}
	
	public static void p (Object line) {
		System.out.println(line);
	}
	
	public static void pa(String[] array) {
		System.out.println(Arrays.toString(array));
	}

}
