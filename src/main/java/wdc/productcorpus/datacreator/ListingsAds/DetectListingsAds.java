package wdc.productcorpus.datacreator.ListingsAds;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;
import wdc.productcorpus.util.StDevStats;

/**
 * @author Anna Primpeli
 * Detect listing pages and ads by considering the number of offer/product elements per page
 * and the standard deviation of their textual description.
 */
public class DetectListingsAds extends Processor<ArrayList<OutputOffer>>{
	
	private File offersFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\offers");
	private int maxOffersPerUrl = 3;
	private double maxStdDevForListingPages = 0.2;
	private File outputClean = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\offers_clean.txt");
	private File outputListings = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\offers_listings");
	private int threads = 1;
	
	private HashSet<OutputOffer> listingsorAddsOffers = new HashSet<OutputOffer>();
	private HashSet<OutputOffer> globalCleanOffers = new HashSet<OutputOffer>();
	private HashSet<OutputOffer> offers = new HashSet<OutputOffer>();
	
	
	@Override
	protected int getNumberOfThreads() {
		return this.threads;
	}
	
	@Override
	protected List<ArrayList<OutputOffer>> fillListToProcess() {
		
		List<ArrayList<OutputOffer>> outputOffersPerURL = new ArrayList<ArrayList<OutputOffer>>();
		
		//import the data 
		DataImporter importOffers = new DataImporter(offersFile);
		offers = importOffers.importOffers();
		
		System.out.printf("Loaded %d offers.\n",offers.size());
		
		//group offers by url
		HashMap<String,List<OutputOffer>> offersByURL = (HashMap<String, List<OutputOffer>>) offers.stream().collect(Collectors.groupingBy(o -> o.getUrl()));
		
		System.out.println("Total number of urls: "+offersByURL.size());
		
		HashMap<String,List<OutputOffer>> offersByListingUls = new HashMap<>();
		for (Map.Entry<String, List<OutputOffer>> offersOfURL : offersByURL.entrySet()) {
			if (offersOfURL.getValue().size()>=maxOffersPerUrl) offersByListingUls.put(offersOfURL.getKey(), offersOfURL.getValue());
			else {
				//even if we dont have lots of offers per url we should ignore the similar or relate offers
				for (OutputOffer o:offersOfURL.getValue()) {
					if (o.getPropertyToParent().toLowerCase().contains("isrelatedto")||o.getPropertyToParent().toLowerCase().contains("issimilarto")) this.listingsorAddsOffers.add(o);
					this.globalCleanOffers.add(o);
				}
				
			}
		}
		
		System.out.printf("Urls with more than %d offers : %d \n", maxOffersPerUrl, offersByListingUls.size());
		
		// we dont need that big object anymore
		offersByURL.clear();		
		
		for (Map.Entry<String, List<OutputOffer>> urlWOffers : offersByListingUls.entrySet()) {
			outputOffersPerURL.add((ArrayList<OutputOffer>) urlWOffers.getValue());
		}
	
		return outputOffersPerURL;
	}
	
	@Override
	protected void process(ArrayList<OutputOffer> offers) throws Exception {
		
		List<OutputOffer> localListingsOrAds = new ArrayList<>();
		List<OutputOffer> localCleanOffers = new ArrayList<>(offers);
		
		
		List<OutputOffer> listingsOrAds = detectListingsOrAdsOfURL(offers);
		localListingsOrAds.addAll(listingsOrAds);
		localCleanOffers.removeAll(localListingsOrAds);
		
		integrateLocalListingsOrAds(localListingsOrAds);
		integrateLocalCleanOffers(localCleanOffers);
	}
	
	
	
	
	private synchronized void integrateLocalCleanOffers(List<OutputOffer> localCleanOffers) {
		this.globalCleanOffers.addAll(localCleanOffers);
		
	}

	private synchronized void integrateLocalListingsOrAds(List<OutputOffer> localListingsOrAds) {
		this.listingsorAddsOffers.addAll(localListingsOrAds);

	}
	
	@Override
	protected void afterProcess() {
		try {
			System.out.println("Listing or ad offers detected: "+this.listingsorAddsOffers.size());
			System.out.println("Clean offers: "+this.globalCleanOffers.size());
			
			System.out.println("Write clean offers");
			writeOffers(this.globalCleanOffers,outputClean);
			System.out.println("Write listing offers");
			writeOffers(this.listingsorAddsOffers, outputListings);
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}
		
	}

	public void detectListingsAds() throws IOException {
		
		//import the data
		DataImporter importOffers = new DataImporter(offersFile);
		HashSet<OutputOffer> offers = importOffers.importOffers();
		
		System.out.printf("Loaded %d offers.\n",offers.size());
		
		//group offers by url
		HashMap<String,List<OutputOffer>> offersByURL = (HashMap<String, List<OutputOffer>>) offers.stream().collect(Collectors.groupingBy(o -> o.getUrl()));
		
		System.out.println("Total number of urls: "+offersByURL.size());
		
		HashMap<String,List<OutputOffer>> offersByListingUls = new HashMap<>();
		for (Map.Entry<String, List<OutputOffer>> offersOfURL : offersByURL.entrySet()) {
			if (offersOfURL.getValue().size()>maxOffersPerUrl) offersByListingUls.put(offersOfURL.getKey(), offersOfURL.getValue());
		}
		
		System.out.printf("Urls with more than %d offers : %d \n", maxOffersPerUrl, offersByListingUls.size());
		
		// we dont need that big object anymore
		offersByURL.clear();
		HashSet<OutputOffer> listingsOrAdsTotal = new HashSet<>();
		
		int urlCount =0;
		
		for (Map.Entry<String, List<OutputOffer>> urlWOffers : offersByListingUls.entrySet()) {
			urlCount++;
			
			if (urlCount%10==0) System.out.println("Parsed "+urlCount+" urls.");
			
			List<OutputOffer> listingsOrAds = detectListingsOrAdsOfURL(urlWOffers.getValue());
			listingsOrAdsTotal.addAll(listingsOrAds);
			
			offers.removeAll(listingsOrAds);
		}
		
		System.out.printf("Found %d listing or ad items. \n",listingsOrAdsTotal.size());
		writeOffers(globalCleanOffers,outputClean);
		writeOffers(listingsOrAdsTotal, outputListings);
	}

	private void writeOffers(HashSet<OutputOffer> offers, File file) throws IOException {
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		for (OutputOffer offer:offers) {
			writer.write(offer.toJSONObject(true)+"\n");
		}
		writer.flush();
		writer.close();
		
	}

	private List<OutputOffer> detectListingsOrAdsOfURL(List<OutputOffer> offers) {
		
		StDevStats stats = new StDevStats();
		
		List<OutputOffer> listingOrAdoffers = new ArrayList<OutputOffer>();
		
		//calculate the stddev of the descriptive length of every entity
		double[] counts = new double[offers.size()];
        int count = 0;
        for(OutputOffer offer : offers){
            counts[count] = (offer.getDescriptivePropertiesAsOneString().length());
            count++;
        }
        
        double stdDev = stats.getStdDev(counts);
       
        
        double median = stats.getMedian(counts);

        
        //if the standard deviation of the length of the offers of one url is minimal then it is probably a listing page
        if (stdDev<maxStdDevForListingPages*median) listingOrAdoffers = offers;

        //if not many items might be ads. Figure this out by considering the relation to the parent element
        else {
        	HashMap<String,List<OutputOffer>> offersByparentItemRelation = (HashMap<String, List<OutputOffer>>) offers.stream().collect(Collectors.groupingBy(o -> o.getPropertyToParent()));
        
           
        	double[] relationCounts = new double[offersByparentItemRelation.size()];
        	count = 0 ;
        	for (Map.Entry<String, List<OutputOffer>> relationOffers : offersByparentItemRelation.entrySet()) {
        		relationCounts[count] = relationOffers.getValue().size();
        		count++; 
        		
        	} 
        	double medianRelationCounts = stats.getMedian(relationCounts);
        
        	for (Map.Entry<String, List<OutputOffer>> relationOffers : offersByparentItemRelation.entrySet()) {
        		//if there are many offers connected to their parent element with the same property
        		if(relationOffers.getValue().size() > medianRelationCounts) listingOrAdoffers.addAll(relationOffers.getValue());
        		else if (relationOffers.getKey().toLowerCase().contains("isrelatedto")||relationOffers.getKey().toLowerCase().contains("issimilarto")) listingOrAdoffers.addAll(relationOffers.getValue());
        	} 
        }
	
		return listingOrAdoffers;
	}
	
	
    public static void main(String args[]) throws IOException {
    	
    	DetectListingsAds detect = new DetectListingsAds();
    	
    	if(args.length>0) {
    		detect.offersFile = new File(args[0]);
    		detect.maxOffersPerUrl = Integer.valueOf(args[1]);
    		detect.maxStdDevForListingPages = Double.valueOf(args[2]);
    		detect.outputClean = new File(args[3]);
    		detect.outputListings = new File(args[4]);
    		detect.threads = Integer.valueOf(args[5]);
    		
    	}
    	
    	//detect.detectListingsAds();
    	detect.process();
    	
    }
}
