package wdc.productcorpus.v2.datacreator.ListingsAds;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.httpclient.URI;
import org.apache.http.client.utils.URLEncodedUtils;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InternetDomainName;

import de.dwslab.dwslib.framework.Processor;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;
import wdc.productcorpus.datacreator.Extractor.SupervisedNode;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;
import wdc.productcorpus.util.DomainUtil;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.util.StDevStats;
import wdc.productcorpus.v2.datacreator.filter.PLDFilterDetect;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.datacreator.filter.DeleteEntities;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;
import wdc.productcorpus.v2.util.StringUtils;

/**
 * @author Anna Primpeli
 * Detect listing pages and ads by considering the number of offer/product elements per page
 * and the standard deviation of their textual description.
 */
public class DetectListingsAds extends Processor<File>{
	
	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory ; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	private int maxOffersPerUrl = 3;
	private double maxStdDevForListingPages = 0.5;
	private double minSimilarity = 0.8;
	
	public boolean negatives = true;
	
	long listingPageCount = (long)0.0;
	long entityListingPageCount = (long)0.0;
	long validEntitiesCount = (long)0.0;
	
	public static void main(String args[]) {
		
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
		String fileName = object.getName().substring(0, object.getName().length()-4); // removes .txt
		BufferedReader br = InputUtil.getBufferedReader(object);
		
		ArrayList<Entity> entities = new ArrayList<Entity>();
		ArrayList<Entity> validEntities = new ArrayList<Entity>();
		ArrayList<Entity> invalidEntities = new ArrayList<Entity>();
		
		String line;
		String currentUrl = "";
		
		long entityListingPageCount = (long)0.0;
		long listingPageCount = (long)0.0;

		while ((line = br.readLine()) != null) {	
			Entity e = EntityStatic.parseEntity(line);
			
			if (e.url.equals(currentUrl)) { // the identifier value
			    entities.add(e);
			} else {
				if(entities.size() > 0) {
					if(isListingPage(entities)) { // check if entities belong to a listing page
						entityListingPageCount += entities.size();
						listingPageCount++;
						
//						if(negatives)
//						invalidEntities.addAll(entities);
					}
					else {
						validEntities.addAll(entities);
					}
				}
				//write once in a while in file so that we don't keep everything in memory
				if (validEntities.size()>100000) {
					CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "listAds", validEntities);
					updateValidEntitiesCount(validEntities.size());
					validEntities.clear();
				}
				entities.clear();
				entities.add(e);
				currentUrl = e.url;
			}
		}
		// process once more for last quads
		if(isListingPage(entities)) { // check if entities belong to a listing page
			entityListingPageCount += entities.size();
			listingPageCount++;
//			if(negatives)
//			invalidEntities.addAll(entities);
		}
		else {
			validEntities.addAll(entities);
		}
		updateListingPageCount(listingPageCount);
		updateEntityListingPageCount(entityListingPageCount);
		updateValidEntitiesCount(validEntities.size());
		CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "listAds", validEntities);
//		if(negatives)
//		CustomFileWriter.writeEntitiesToFile("negatives", outputDirectory, "", invalidEntities);
		
	}
	
	// sync error count with global error count
	private synchronized void updateListingPageCount(long count) {
		this.listingPageCount += count;
	}
	
	// sync error count with global error count
	private synchronized void updateEntityListingPageCount(long count) {
		this.entityListingPageCount += count;
	}
	
	// sync error count with global error count
	private synchronized void updateValidEntitiesCount(long count) {
		this.validEntitiesCount += count;
	}
	
	@Override
	protected void afterProcess() {
		System.out.println("Valid Entities: " + validEntitiesCount);
		System.out.println("Listing Page Entities: " + entityListingPageCount);
		System.out.println("Listing Pages: " + listingPageCount);
	}
	
	
	public boolean isListingPage(ArrayList<Entity> entities) {
		if(entities.get(0).itemPage == true) {
			return false;
		}
		if(entities.size() >= maxOffersPerUrl) { // if the number of entities is bigger => potential listing page candidate			
			String price = entities.get(0).price; // price of the first entity of a page
			boolean samePrice = true;
			for(Entity e : entities) {
				if(e.isVariation == true) { // when subids is set there is product variations and not a listing page
					return false;
				}
				if(e.price == null || (e.price != null && (!e.price.equals(price) || StringUtils.containsOnlyZeros(e.price)))) {
					samePrice = false;
				}
			}
			if(samePrice) { // if same price across all entities on a webpage likely no listing page
				return false;
			}	
			return true;
		}
		
		return false;
	}
	
	/**
	 * Attention: <br>
	 * This filter is only good for urls that have more than 3 entities
	 * 
	 * This method checks if a url is potentially a listing page. Two main assumptions:
	 * 1.) If the url contains to many url params its a listing page
	 * 2.) if the url has only one path paramter its also likely to be listing pagey<br>
	 * 
	 * 
	 * 
	 * @param url
	 * @return true if the url is likely to be listing page
	 */
	public static boolean checkUrlForListing(String url) {
		if(toManyUrlParamters(url)) {
			return true;
		}
		if(toShortResource(url)) {
			return true;
		}
		return false;
	}
	
	/**
	 * Checks if the entities could potentially have one main product and the others are variations
	 * @param entities
	 * @return
	 */
	private boolean checkForMainProduct(ArrayList<Entity> entities) {
		return false;
	}
	
	private static boolean toShortResource(String url_) {
		try {
			URL url = new URL(url_);
			String path = url.getPath();
			int count = path.length() - path.replace("/", "").length();
			if(count <= 1) { // example.com/myproduct.html is very likely to not be single product
				return true;
			}
		} catch (MalformedURLException e) {}
		return false;
	}
	
	private static boolean toManyUrlParamters(String url) {
		int number = 0;
		try {
			number = splitQuery(new URL(url)).size();
		} catch (Exception e) {}
		if(number > 2) { // if 3 or more url params
			//PrintUtils.p(number + "   " + entitiesAmount + "   " + url);
			return true;
		}
		
		return false;
	}
	
	/**
	 * Gets all Url parameters
	 * @param url
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static Map<String, String> splitQuery(URL url) throws Exception {
	    Map<String, String> query_pairs = new LinkedHashMap<String, String>();
	    String query = url.getQuery();
	    String[] pairs = query.split("&");
	    for (String pair : pairs) {
	        int idx = pair.indexOf("=");
	        query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
	    }
	    return query_pairs;
	}
	
	private int getUrlParamAmount(String url) {
		int amount = 0;
		try {
			amount = splitQuery(new URL(url)).size();
		} catch (Exception e) {}
		return amount;
	}
	
	
	
	
	
	
}
