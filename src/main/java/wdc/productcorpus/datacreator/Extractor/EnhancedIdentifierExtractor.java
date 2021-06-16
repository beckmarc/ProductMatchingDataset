package wdc.productcorpus.datacreator.Extractor;

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
import java.util.stream.Collectors;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

import org.apache.commons.lang.StringEscapeUtils;
import org.eclipse.jdt.core.dom.InstanceofExpression;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Edge;
import ldif.entity.Node;
import ldif.local.datasources.dump.QuadFileLoader;
import ldif.runtime.Quad;
import scala.actors.threadpool.Arrays;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.model.ParentEntity;
import wdc.productcorpus.v2.model.ParseEntityResult;
import wdc.productcorpus.v2.model.ParseEntityResult.Type;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;
import wdc.productcorpus.v2.util.QuadParser;
import wdc.productcorpus.v2.util.StringUtils;

/**
 
 *
 */
@Parameters(commandDescription = "")
public class EnhancedIdentifierExtractor extends Processor<File> {

	@Parameter(names = { "-out",
			"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 

	@Parameter(names = { "-in",
			"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;

	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;


	ArrayList<String> identifiers = new ArrayList<String>() {{
	    add("/gtin12>");
	    add("/gtin13>");
	    add("/gtin14>");
	    add("/gtin8>");
	    add("/mpn>");
	    add("/productID>");
	    add("/sku>");
	    add("/identifier>");
	    add("/serialNumber>");
	    add("/gtin>");
	}};
	
	ArrayList<String> textualProperties = new ArrayList<String>() {{
	    add("/brand>");
	    add("/name>");
	    add("/title>");
	    add("/description>");
	    add("/price>");
	    add("/priceCurrency>");
	    add("/image>");
	    add("/availability>");
	    add("/manufacturer>");
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

	private long errorCount = 0;
	private long parsedLines = 0;
	private long entitiesCount = 0;
	

	@Override
	protected void process(File object) throws Exception {
		
		String fileName = CustomFileWriter.removeExt(object.getName());
				
		long parsedLines = 0;
		long entitiesCount = 0;
		long errorCount = 0;

		int searchPages = 0;
		int realestatePages = 0;
		int collectionPages = 0;
		int checkoutPages = 0;

		QuadParser qp = new QuadParser(true);
		String currentUrl = "";
		String currentLine = "";
		List<Quad> quads = new ArrayList<Quad>();
		// read the file
		BufferedReader br = InputUtil.getBufferedReader(object);
		ArrayList<Entity> nodesWithSupervision = new ArrayList<Entity>();
		
		while (br.ready()) {
			try {

				currentLine = br.readLine();
				Quad q = qp.parseQuadLine(currentLine);
				parsedLines++;
				
				if(parsedLines % 1000000 == 0) {
					System.out.println("Parsed "+parsedLines+" from file:"+object.getName());
				}
				
				if (q != null && q.graph().equals(currentUrl)) {
					quads.add(q);
				} else {
					if (quads.size() > 0) {
						ParseEntityResult result = parseEntitiesPerUrl(quads, object.getName());
						if(result.entities != null && !result.entities.isEmpty()) {
							nodesWithSupervision.addAll(result.entities);
						}
										
						//write once in a while in file so that we don't keep everything in memory
						if (nodesWithSupervision.size()>100000) {
							CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "extract", nodesWithSupervision);
							PrintUtils.p("100000 entities printed");
							entitiesCount += nodesWithSupervision.size();
							nodesWithSupervision.clear();
						}
					}
					quads.clear();
					quads.add(q);
					currentUrl = q.graph();
				}
			} catch (Exception e) {
				errorCount++;
//				PrintUtils.p(currentLine);
//				e.printStackTrace();
			}
		}
		// process once more for the last quads
		if (quads.size() > 0) {
			ParseEntityResult result = parseEntitiesPerUrl(quads, object.getName());
			if(result.entities != null && !result.entities.isEmpty()) {
				nodesWithSupervision.addAll(result.entities);
			}
		}
		
		br.close();
		updateEntitiesCount(entitiesCount + nodesWithSupervision.size());
		updateErrorCount(errorCount);
		updateLineCount(parsedLines);
		
		CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "extract", nodesWithSupervision);
		// writeErrorsInFile("errors.txt", errorLines);
	}


	private ParseEntityResult parseEntitiesPerUrl(List<Quad> quads, String fileName) throws Exception {
		ParseEntityResult parseEntityResult = new ParseEntityResult();
		parseEntityResult.entities = new ArrayList<Entity>();
	
		List<Quad> quadsPerEntity = new ArrayList<Quad>();
		
		HashMap<String,Entity> entities = new HashMap<String,Entity>();		// mapped nodeid -> entity	
		ArrayList<String> similarRelatedNodes = new ArrayList<String>();
		HashMap<String, ArrayList<String>> offers = new HashMap<String, ArrayList<String>>();
		HashMap<String, ArrayList<String>> itemOffered = new HashMap<String, ArrayList<String>>();
		//HashSet<String> items = new HashSet<String>();
		
		String mainEntityId = null;
		boolean isItemPage = false;
		
		String currentId = "";
		quads.add(blankQuad());
		
		for(Quad outerQ: quads) {
			if(currentId.equals(outerQ.subject().value())) {
				quadsPerEntity.add(outerQ);
			} else {
				Entity entity = new Entity();
				entity.hasId = false;
				
				if(quadsPerEntity.size() > 0) { 
					for (Quad q: quadsPerEntity){ 
						
						for (String id:identifiers){
							if (q.predicate().contains(id) && !q.value().value().trim().equals("")) {
								EntityStatic.setIdentifyingProperty(entity, id, q.value().value());
								entity.hasId = true;
							}	
						}
						
						for (String textProp:textualProperties) { // to do: fix price contains (priceValidUntil)
							//get the text of literal of object of the predicates we have defined
//							PrintUtils.p(q.predicate());
//							PrintUtils.p(textProp);
//							PrintUtils.p(q.predicate().contains(textProp));
							if (q.predicate().contains(textProp) && q.value().toString().startsWith("\"") ) {			
								EntityStatic.setTextualProperty(entity, textProp, normalizeText(q.value().value()));	
							}
						}
						
						String predicate = q.predicate().toLowerCase();
						String object = q.value().toString().toLowerCase();
						
						if(object.contains("/searchresultspage") && isMainPage(q)) {
							return new ParseEntityResult(ParseEntityResult.Type.SEARCHRESULTPAGE);
						}
						
						if(object.contains("/realestatelisting") && isMainPage(q)) {
							return new ParseEntityResult(ParseEntityResult.Type.REALESTATELISTING);
						}
						
						if(object.contains("/collectionpage") && isMainPage(q)) {
							return new ParseEntityResult(ParseEntityResult.Type.COLLECTIONPAGE);
						}
						
						if(object.contains("/checkoutpage") && isMainPage(q)) {
							return new ParseEntityResult(ParseEntityResult.Type.CHECKOUTPAGE);
						}
						
						if(object.contains("/itempage") && isMainPage(q)) {
							isItemPage = true;
						}
						
						if(object.contains("/product")) {
							entity.isProduct = true;
						}
						
						if(object.contains("/offer")) {
							entity.isOffer = true;
						}					
								
						if (predicate.contains("/offers")) {
							addTransitive(offers, q.subject().toString(), q.value().toString());
						}
						
						if (predicate.contains("/itemoffered")) {
							// inverse relationship of offers
							addTo(itemOffered, q.subject().toString(), q.value().toString());
						}
						
						if (predicate.contains("/mainentity")) {
							// ( page -> entity )
							mainEntityId = q.value().toString();
						}
						
						if (predicate.contains("/mainentityofpage")) {
							// inverse relationship of mainEntity (entity -> page)
							mainEntityId = q.subject().toString();
						}
									
						if(predicate.contains("/issimilarto") || predicate.contains("/isrelatedto")) {
							// These nodes are likely to not be the main entity of the page
							similarRelatedNodes.add(q.value().toString());
						}
						
					}

					entity.nodeId = quadsPerEntity.get(0).subject().toString();				
					entities.put(entity.nodeId, entity);
					
				}
				quadsPerEntity.clear();
				quadsPerEntity.add(outerQ);
				currentId = outerQ.subject().value();
			}
		}

		// process entities based on information gathered before	
		for(Map.Entry<String, Entity> entry : entities.entrySet()) { 

			processOffersRelationship(entry, offers, entities);
			processItemOfferedRelationship(entry, itemOffered, entities);
			processSimilarRelatedRelationship(entry, similarRelatedNodes);
//			processItems(entry, items);
			
		}
		
		if(mainEntityId != null) { // if we have found a main entity
			Entity mainEntity = entities.get(mainEntityId);
			if(mainEntity != null && mainEntity.hasId == true && (mainEntity.name != null || mainEntity.description != null)) { // select only if it has identifier
				mainEntity.fileId = fileName;
				mainEntity.url = quads.get(0).graph();
				parseEntityResult.entities.add(mainEntity);
			}
		} else {
			for(Map.Entry<String, Entity> entry : entities.entrySet()) { // for every entity check the conditions
				Entity entity = entry.getValue();
				if(entity.hasId == true && (entity.name != null || entity.description != null)) {
					entity.fileId = fileName;
					entity.url = quads.get(0).graph();
					parseEntityResult.entities.add(entity);
				}
			}
		}
		
		
		if(isItemPage && !parseEntityResult.entities.isEmpty()) {
			parseEntityResult.entities.get(0).itemPage = true;
		}
		//PrintUtils.p(parseEntityResult.entities.size());
		return parseEntityResult;
	}
	
	/**
	 * Determines wether a quad can be the main page or just a link
	 * @param q
	 * @return
	 */
	private boolean isMainPage(Quad q) {
		return q.subject().toString().contains(q.graph());
	}
	
	/**
	 * Approach for handling transitive node relationship
	 * @param map
	 * @param key
	 * @param value
	 */
	private void addTransitive(HashMap<String, ArrayList<String>> map, String key, String value) {	
		HashMap<String, String> transitives = new HashMap<String, String>();
		for(Map.Entry<String, ArrayList<String>> entry : map.entrySet()) {
			for(Iterator<String> it = entry.getValue().iterator(); it.hasNext();) {
				if(key.equals(it.next())) {
					transitives.put(entry.getKey(), value);
				}
			}
		}
		if(!map.containsKey(key))
		map.put(key, new ArrayList<String>());
		map.get(key).add(value);
		for(Map.Entry<String,String> entry : transitives.entrySet()) {
			map.get(entry.getKey()).add(entry.getValue());
		}
		
	}
	
	/**
	 * Simply adds more information to an entity (May be used for the listing heuristic)
	 * @param entry
	 * @param items
	 */
	private void processItems(Map.Entry<String, Entity> entry, HashSet<String> items) {
		if(items.contains(entry.getKey())) {
			entry.getValue().isItem = true;
		}
	}
	
	/**
	 * Filters out entities marked as similarto or relatedto
	 * @param entry
	 * @param similarRelatedNodes
	 */
	private void processSimilarRelatedRelationship (
			Map.Entry<String, Entity> entry,
			ArrayList<String> similarRelatedNodes) {
		if(similarRelatedNodes.contains(entry.getKey())) {
			entry.getValue().hasId = false;
		}
	}
	
	private void processOffersRelationship (
			Map.Entry<String, Entity> entry, // the currently looked at entity 
			HashMap<String, ArrayList<String>> offers, // the offers found
			HashMap<String,Entity> entities ) { // mapped nodeid -> entity
		
		if(offers.containsKey(entry.getKey())) {
			//PrintUtils.p(entry.getKey());
			ArrayList<String> childIds = offers.get(entry.getKey()); // child entity according to offers relation
			if(childIds.size() == 1 && entities.get(childIds.get(0)) != null) { // direct relation product to offer
				EntityStatic.mergeProductAndOffer(entry.getValue(), entities.get(childIds.get(0)));
			}
			else if(childIds.size() > 1 && entities.get(childIds.get(0)) != null){ // if the entity has multiple direct children
				for(String childId : childIds) {
					Entity childEntity = entities.get(childId);
					if(childEntity != null) {
						childEntity.isVariation = true;
						EntityStatic.mergeEntities(entry.getValue(), childEntity);
					}
				}
			}
		}
	}
	
	private void processItemOfferedRelationship (
			Map.Entry<String, Entity> entry,
			HashMap<String, ArrayList<String>> itemOffered,
			HashMap<String,Entity> entities ) {
		
		if(itemOffered.containsKey(entry.getKey())) {
			//PrintUtils.p(entry.getKey());
			ArrayList<String> childIds = itemOffered.get(entry.getKey()); // child entity according to itemOffered relation
			if(childIds.size() == 1 && entities.get(childIds.get(0)) != null && entities.get(childIds.get(0)).isProduct ) { // direct relation offer to product
				EntityStatic.mergeProductAndOffer(entities.get(childIds.get(0)), entry.getValue());
			}
			else if(childIds.size() > 1){ // if the entity has multiple direct children
				for(String childId : childIds) {
					Entity childEntity = entities.get(childId);
					if(childEntity != null) {
						childEntity.isVariation = true;
						EntityStatic.mergeEntities(entry.getValue(), childEntity);
					}
				}
			}
		}
	}
	
	/**
	 * Adds the specified value to the arraylist that is the value of the key.<br>
	 * If there is no entry for the key creates an arraylist and adds the value
	 * @param map
	 * @param key
	 * @param value
	 */
	private void addTo(HashMap<String, ArrayList<String>> map, String key, String value) {
		if(!map.containsKey(key))
		map.put(key, new ArrayList<String>());
		map.get(key).add(value);
	}
	
	private Quad blankQuad() throws Exception {
		return new Quad(Node.createBlankNode("", null), "", Node.createBlankNode("", null), "");
	}
	
	/**
	 * Normalizes textual properties. Returns null if the string is empty after the process.
	 * @param text
	 * @return
	 */
	public String normalizeText(String text) throws Exception {
		String normalizedText = text.replaceAll("(\\\\n)|(\\\\r)|(\\\\t)|(\ufffd)", ""); 
		normalizedText = normalizedText.replaceAll("(\\s{2,})", ""); 
		normalizedText = normalizedText.replaceAll("\u00A0", "").trim(); // \ufffd are the question mark characters
		normalizedText = StringEscapeUtils.unescapeHtml(normalizedText); // unescapes html text
		normalizedText = normalizedText.replaceAll("\\<.*?>","");
		if(normalizedText.equals("") || !StringUtils.hasAlphaDigit(normalizedText)) {
			return null;
		}
		return normalizedText;
	}
	
	public static String normalizeValue(String rawValue) {
		String normalizedValue = rawValue.toLowerCase();
		return normalizedValue;
	}

	// sync error count with global error count
	private synchronized void updateErrorCount(long errorCount) {
		this.errorCount += errorCount;
	}

	// sync line count with global line count
	private synchronized void updateLineCount(long lineCount) {
		this.parsedLines += lineCount;
	}
	
	// sync error count with global error count
	private synchronized void updateEntitiesCount(long errorCount) {
		this.entitiesCount += errorCount;
	}
	

	@Override
	protected void afterProcess() {
		try {
			System.out.println("Parsed " + parsedLines + " lines.");
			System.out.println("Could not parse " + errorCount + " lines (quads).");
			System.out.println("Entities with supervision: "+entitiesCount);

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

class Pair {
	public String nodeId1;
	public String nodeId2;
	
	public Pair(String nodeId1, String nodeId2) {
		this.nodeId1 = nodeId1;
		this.nodeId2 = nodeId2;
	}
}
