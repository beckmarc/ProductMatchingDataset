package wdc.productcorpus.datacreator;


import com.beust.jcommander.JCommander;

import wdc.productcorpus.datacreator.ClusterCreator.GraphCreator;
import wdc.productcorpus.datacreator.Extractor.EnhancedIdentifierExtractor;
import wdc.productcorpus.datacreator.Extractor.IdentifierExtractor;
import wdc.productcorpus.datacreator.Extractor.OrderVerifier;
import wdc.productcorpus.datacreator.Filter.ClassFilter;
import wdc.productcorpus.datacreator.Filter.FilterCombination;
import wdc.productcorpus.datacreator.Filter.TextualValueFilter;
import wdc.productcorpus.datacreator.Filter.ValuesListClassFilter;
import wdc.productcorpus.datacreator.MissingValues.MissingValuesFill;
import wdc.productcorpus.datacreator.Profiler.IDInfoProfiler;
import wdc.productcorpus.datacreator.Profiler.PropFrequencyPerValue;
import wdc.productcorpus.datacreator.Profiler.PropFrequencyPerValueJSON;
import wdc.productcorpus.util.DeduplicatorSerial;
import wdc.productcorpus.v2.util.SamplerDetect;
import wdc.productcorpus.util.ValueSearch;
import wdc.productcorpus.v2.cluster.OfferMerger;
import wdc.productcorpus.v2.cluster.OfferSampler;
import wdc.productcorpus.v2.cluster.OfferSamplerEnhanced;
import wdc.productcorpus.v2.cluster.OfferSorter;
import wdc.productcorpus.v2.datacreator.ListingsAds.DetectListingsAds;
import wdc.productcorpus.v2.datacreator.filter.BadIdentifierFilter;
import wdc.productcorpus.v2.datacreator.filter.PLDFilterDetect;
import wdc.productcorpus.v2.datacreator.filter.PLDFilterDetectEnhanced;
import wdc.productcorpus.v2.datacreator.filter.VariationDetectFilter;
import wdc.productcorpus.v2.profiler.CorpusProfiler;
import wdc.productcorpus.v2.profiler.MasterProfiler;
import wdc.productcorpus.v2.datacreator.filter.ExampleFilter;
import wdc.productcorpus.v2.datacreator.filter.MainEntityFilter;
import wdc.productcorpus.v2.util.ClearFile;

public class Master {

	public static void main(String[] args) {

		Master master = new Master();
		JCommander jc = new JCommander(master);
		
		/** Training Dataset Creation *****************************************/
		MissingValuesFill missingValues = new MissingValuesFill();
		EnhancedIdentifierExtractor id_extract_enhanced = new EnhancedIdentifierExtractor(); // 1. step	
		IdentifierExtractor id_extract = new IdentifierExtractor();
		ValuesListClassFilter filter_classes_Wvalues = new ValuesListClassFilter(); // optional ?
		PLDFilterDetect filter_plds = new PLDFilterDetect(); // expects Json Format
		GraphCreator createGraph = new GraphCreator(); 
		BadIdentifierFilter filterIdentifiers = new BadIdentifierFilter(); 
		ExampleFilter filterDescription = new ExampleFilter();
		DetectListingsAds detectListingAds = new DetectListingsAds();
		VariationDetectFilter variationDetect = new VariationDetectFilter();
		MainEntityFilter mainEntity = new MainEntityFilter();
		PLDFilterDetectEnhanced plde = new PLDFilterDetectEnhanced();
		
		
		/** Training Dataset Profiling *****************************************/
		PropFrequencyPerValue profile_property = new PropFrequencyPerValue();
		PropFrequencyPerValueJSON profile_propertyJSON = new PropFrequencyPerValueJSON();
		IDInfoProfiler profile = new IDInfoProfiler();
		OfferSorter offerSorter = new OfferSorter();
		OfferSamplerEnhanced offerSimpleSorter = new OfferSamplerEnhanced();
		OfferMerger offerMerger = new OfferMerger();
		CorpusProfiler cp = new CorpusProfiler();
		OfferSampler offerSampler = new OfferSampler();
		MasterProfiler mp = new MasterProfiler();
		OfferSamplerEnhanced ose = new OfferSamplerEnhanced();
		
		/** Gold Standard ********************************/
		FilterCombination combine_filter = new FilterCombination(); // gold standard
		TextualValueFilter filter_text = new TextualValueFilter(); // gold standard
		ClassFilter filter_classes = new ClassFilter(); // gold standard
		
		/** Utility Classes *****************************************/
		SamplerDetect sample = new SamplerDetect();
		ClearFile clear_file = new ClearFile();
		ValueSearch searchValue = new ValueSearch();
		
		wdc.productcorpus.datacreator.ListingsAds.DetectListingsAds detectListingAdsOld = new wdc.productcorpus.datacreator.ListingsAds.DetectListingsAds();
		
		
		jc.addCommand("extractNodesWithIDs", id_extract);
		jc.addCommand("filterClasses", filter_classes);
		jc.addCommand("filterTextualValues", filter_text);
		jc.addCommand("filterCombination", combine_filter);
		jc.addCommand("createGraph", createGraph);
		jc.addCommand("profile", profile);
		jc.addCommand("searchValue", searchValue);
		jc.addCommand("filterClassesWithValues", filter_classes_Wvalues);
		jc.addCommand("sample", sample);
		jc.addCommand("filterplds", filter_plds);
		jc.addCommand("profileProperty", profile_property);
		jc.addCommand("profilePropertyJSON", profile_propertyJSON);
		
		jc.addCommand("clearFile", clear_file);
		jc.addCommand("filterIdentifiers", filterIdentifiers);
		jc.addCommand("missingValues", missingValues);
		jc.addCommand("filterExamples", filterDescription);
		jc.addCommand("detectListingAds", detectListingAds);
		jc.addCommand("detectListingAdsOld", detectListingAdsOld);
		jc.addCommand("id_extract_enhanced", id_extract_enhanced);
		jc.addCommand("variationDetect", variationDetect);
		jc.addCommand("mainEntity", mainEntity);
		jc.addCommand("plde", plde);
		
		jc.addCommand("offerSorter", offerSorter);
		jc.addCommand("offerSimpleSorter", offerSimpleSorter);
		jc.addCommand("offerMerger", offerMerger);
		jc.addCommand("cp", cp);
		jc.addCommand("offerSampler", offerSampler);
		jc.addCommand("mp", mp);
		jc.addCommand("ose", ose);
		
		
		
		try {

			jc.parse(args);
			switch (jc.getParsedCommand()) {
			case "extractNodesWithIDs":
				id_extract.process();
				break;
			case "filterClasses":
				filter_classes.process();
				break;
			case "filterTextualValues":
				filter_text.process();
				break;
			case "filterCombination":
				combine_filter.process();
				break;
			case "filterplds":
				filter_plds.process();
				break;
			case "profile":
				profile.process();
				break;
			case "searchValue":
				searchValue.process();
				break;
			case "filterClassesWithValues":
				filter_classes_Wvalues.process();
				break;
			case "sample":
				sample.process();
				break;
			case "profileProperty":
				profile_property.process();
				break;
			case "profilePropertyJSON":
				profile_propertyJSON.process();
				break;
			case "clearFile":
				clear_file.process();
				break;
			case "createGraph":
				createGraph.process();
				break;
			case "filterIdentifiers":
				filterIdentifiers.process();
				break;
			case "missingValues":
				missingValues.process();
				break;
			case "filterExamples":
				filterDescription.process();
				break;
			case "detectListingAds":
				detectListingAds.process();
				break;
			case "detectListingAdsOld":
				detectListingAdsOld.process();
				break;
			case "id_extract_enhanced":
				id_extract_enhanced.process();
				break;
			case "offerMerger":
				offerMerger.process();
				break;
			case "offerSorter":
				offerSorter.process();
				break;
			case "offerSimpleSorter":
				offerSimpleSorter.process();
				break;
			case "variationDetect":
				variationDetect.process();
				break;
			case "mainEntity":
				mainEntity.process();
				break;
			case "plde":
				plde.process();
				break;
			case "cp":
				cp.process();
				break;
			case "offerSampler":
				offerSampler.process();
				break;
			case "mp":
				mp.process();
				break;
			case "ose":
				ose.process();
				break;
			}

		} catch (Exception pex) {
			if (jc.getParsedCommand() == null) {
				jc.usage();
			} else {
				switch (jc.getParsedCommand()) {
				case "extractNodesWithIDs":
					new JCommander(id_extract).usage();
					break;				
				case "filterClasses":
					new JCommander(filter_classes).usage();
					break;
				case "filterTextualValues":
					new JCommander(filter_text).usage();
					break;
				case "filterCombination":
					new JCommander(combine_filter).usage();
					break;
				case "filterplds":
					new JCommander(filter_plds).usage();
					break;
				case "profile":
					new JCommander(profile).usage();
					break;
				case "searchValue":
					new JCommander(searchValue).usage();
					break;
				case "filterClassesWithValues":
					new JCommander(filter_classes_Wvalues).usage();
					break;
				case "sample":
					new JCommander(sample).usage();
					break;
				case "profileProperty":
					new JCommander(profile_property).usage();
					break;
				case "profilePropertyJSON":
					new JCommander(profile_propertyJSON).usage();
					break;
				case "clearFile":
					new JCommander(clear_file).usage();
					break;
				case "createGraph":
					new JCommander(createGraph).usage();
					break;
				case "filterIdentifiers":
					new JCommander(filterIdentifiers).usage();
					break;
				case "missingValues":
					new JCommander(missingValues).usage();
					break;
				case "filterExamples":
					new JCommander(filterDescription).usage();
					break;
				case "detectListingAds":
					new JCommander(detectListingAds).usage();
					break;
				case "detectListingAdsOld":
					new JCommander(detectListingAdsOld).usage();
					break;
				case "id_extract_enhanced":
					new JCommander(id_extract_enhanced).usage();
					break;
				case "offerSorter":
					new JCommander(offerSorter).usage();
					break;
				case "offerSimpleSorter":
					new JCommander(offerSimpleSorter).usage();
					break;
				case "offerMerger":
					new JCommander(offerMerger).usage();
					break;
				case "variationDetect":
					new JCommander(variationDetect).usage();
					break;
				case "mainEntity":
					new JCommander(mainEntity).usage();
					break;
				case "plde":
					new JCommander(plde).usage();
					break;
				case "cp":
					new JCommander(cp).usage();
					break;
				case "offerSampler":
					new JCommander(offerSampler).usage();
					break;
				case "mp":
					new JCommander(mp).usage();
					break;
				case "ose":
					new JCommander(ose).usage();
					break;
					
				default:
					jc.usage();
				}
			}
		}

	}

}
