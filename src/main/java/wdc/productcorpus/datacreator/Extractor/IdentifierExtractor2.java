package wdc.productcorpus.datacreator.Extractor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import ldif.local.datasources.dump.QuadFileLoader;
import ldif.runtime.Quad;
import scala.actors.threadpool.Arrays;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;
import wdc.productcorpus.v2.util.QuadParser;
import wdc.productcorpus.v2.util.StringUtils;

/**
 
 *
 */
@Parameters(commandDescription = "")
public class IdentifierExtractor2 extends Processor<File> {

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

	@Override
	protected void process(File object) throws Exception {

//		OrderVerifier verify = new OrderVerifier();
//		if (!verify.isOrderedBySubjectID(object.getPath())) {
//			System.out.println("File "+object.getPath()+ " is not ordered. Process will end.");
//			System.exit(0);
//		}
				
//		System.out.println("File ordered: "+object.getName());
		long lineCount = (long)0.0;
		int errorCount = 0;

		QuadFileLoader qfl = new QuadFileLoader();
		QuadParser qp = new QuadParser(true);
		String currentID = "";
		List<Quad> quads = new ArrayList<Quad>();
		// read the file
		BufferedReader br = InputUtil.getBufferedReader(object);
		
		ArrayList<SupervisedNode> nodesWithSupervision = new ArrayList<SupervisedNode>();
		
		int nodesWithSupervisionCount = 0;
		Quad eq = null;
		String currentLine = "";
		ArrayList<String> errorLines = new ArrayList<String>();
		
		while (br.ready()) {
			try {
				//Quad q = qfl.parseQuadLine(br.readLine());
				currentLine = br.readLine();
				Quad q = qp.parseQuadLine(currentLine);
				eq = q;
				lineCount++;
				
				if(lineCount % 1000000 == 0) {
					System.out.println("Parsed "+lineCount+" from file:"+object.getName());
				}
				
				if (q.subject().toString().equals(currentID)) {
					quads.add(q);
				} else {
					if (quads.size() > 0) {
						PrintUtils.p(quads.size());
						nodesWithSupervision.addAll(filterQuadsOfSubject(quads, object.getName()));
						
						//write once in a while in file so that we don't keep everything in memory
						if (nodesWithSupervision.size()>100000) {
							CustomFileWriter.writeNodesToFile(object.getName(), outputDirectory, "extract", nodesWithSupervision);
							nodesWithSupervisionCount += nodesWithSupervision.size();
							nodesWithSupervision.clear();
						}
					}
					quads.clear();
					quads.add(q);
					currentID = q.subject().toString();
				}
			} catch (Exception e) {
//				e.printStackTrace();
//				System.out.println(currentLine);
//				System.out.println(eq);
				errorLines.add(currentLine);
				errorCount++;
			}
		}
		// process once more for the last quads
		if (quads.size() > 0) {
			nodesWithSupervision.addAll(filterQuadsOfSubject(quads, object.getName()));
		}
		br.close();
		nodesWithSupervisionCount += nodesWithSupervision.size();

		System.out.println("Nodes with supervision: "+nodesWithSupervisionCount+" in file:"+object.getName());
		updateErrorCount(errorCount);
		updateLineCount(lineCount);
		String fileName = CustomFileWriter.removeExt(object.getName());
		CustomFileWriter.writeNodesToFile(fileName, outputDirectory, "extract", nodesWithSupervision);
		// writeErrorsInFile("errors.txt", errorLines);
	}

	
	private ArrayList<SupervisedNode> filterQuadsOfSubject(List<Quad> quads, String fileName) {
		
		ArrayList<SupervisedNode> nodesWithIDs = new ArrayList<SupervisedNode>();
		
		HashMap<String, List<Quad>> quadsPerEntity = new HashMap<String, List<Quad>>();
		
		//in order to identify the entity we need the url and the subject id
		for (Quad q:quads) {
			String id = q.subject().toString()+"_"+q.graph();
			
			List<Quad> existingQuads = quadsPerEntity.get(id);
			if (null == existingQuads) existingQuads = new ArrayList<Quad>();
			existingQuads.add(q);
			quadsPerEntity.put(id, existingQuads);
		}
		
		PrintUtils.p("size:" +quadsPerEntity.keySet().size());
		
		
		for (Map.Entry<String, List<Quad>> q:quadsPerEntity.entrySet()){
			boolean hasID = false;
			
			String nodeid="";
			String url="";
			HashMap<String,String> textPerProp = new HashMap<String,String>();
			HashMap<String, String> idProperty_Value = new HashMap<String,String>();
			
			// Textual Properties
			String title="";
			String name="";
			String description="";
			String brand="";
			
			for (Quad equad:q.getValue()) {
				
				nodeid = equad.subject().toString();
				url = equad.graph();
				
				for (String id:identifiers){			
					if (equad.predicate().contains(id)) {
						hasID = true;
						idProperty_Value.put(id, normalizeValue(equad.value().toString()));						
					}
				}
				
				for (String textProp:textualProperties) {
					//get the text of literal of object of the predicates we have defined			
					if (equad.predicate().toString().contains(textProp) && equad.value().toString().startsWith("\"") ) {
						switch(textProp) {
							case "/title":
								title = equad.value().value().toString();
							break;
							case "/name":
								name = equad.value().value().toString();
							break;
							case "/description":
								description = equad.value().value().toString();
							break;
							case "/brand":
								brand = equad.value().value().toString();
							break;
						}
					}
				}
			}
			
			//if we found an interesting entity store it
			if (hasID) {
				for (Map.Entry<String, String> idV:idProperty_Value.entrySet()) {
					nodesWithIDs.add(new SupervisedNode(nodeid, url, fileName, idV.getKey(), idV.getValue(),
							"\""+name+"\"", "\""+title+"\"", "\""+brand+"\"", "\""+description+"\""));
				}
			}
		}
		return nodesWithIDs;
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

	

	@Override
	protected void afterProcess() {
		try {
			System.out.println("Parsed " + parsedLines + " lines.");
			System.out.println("Could not parse " + errorCount + " lines (quads).");

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
