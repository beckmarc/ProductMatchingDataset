package wdc.productcorpus.datacreator;

import com.beust.jcommander.JCommander;

import wdc.productcorpus.datacreator.OutputFilesCreator.EnglishCorpusFilter;
import wdc.productcorpus.datacreator.OutputFilesCreator.HTMLCombiner;
import wdc.productcorpus.datacreator.OutputFilesCreator.IDClusterInfoFetcher;
import wdc.productcorpus.datacreator.Profiler.Categories.Categorizer;

/**
 * @author Anna Primpeli
 * Considers different input files to construct the final files for our website
 * Takes the nodeID-url from the created clusters. 
 * Fetches the relevant triples from the initial Product.nt file.
 */
public class FilesCreator {
	public static void main(String[] args) {

		FilesCreator create = new FilesCreator();
		JCommander jc = new JCommander(create);
		
		IDClusterInfoFetcher fetch = new IDClusterInfoFetcher();
		HTMLCombiner combine = new HTMLCombiner();
		EnglishCorpusFilter filterEnglish = new EnglishCorpusFilter();
		Categorizer categorize = new Categorizer();
		
		jc.addCommand("createOutputFiles", fetch);
		jc.addCommand("combine", combine);
		jc.addCommand("filterEnglish", filterEnglish);
		jc.addCommand("categorizeClusters", categorize);


		try {

			jc.parse(args);
			switch (jc.getParsedCommand()) {
			case "createOutputFiles":
				fetch.process();
				break;			
			case "combine":
				combine.process();
				break;
			case "filterEnglish":
				filterEnglish.process();
				break;
			case "categorizeClusters":
				categorize.categorize();
				break;
			}
			

		} catch (Exception pex) {
			if (jc.getParsedCommand() == null) {
				jc.usage();
			} else {
				switch (jc.getParsedCommand()) {
				case "createOutputFiles":
					new JCommander(fetch).usage();
					break;
				case "combine":
					new JCommander(combine).usage();
					break;
				case "filterEnglish":
					new JCommander(filterEnglish).usage();
					break;
				case "categorizeClusters":
					new JCommander(categorize).usage();
					break;
				default:
					jc.usage();
				}
				}
			}
	}
		
}
