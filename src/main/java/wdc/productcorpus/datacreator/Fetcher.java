package wdc.productcorpus.datacreator;

import com.beust.jcommander.JCommander;

import wdc.productcorpus.datacreator.CorpusExtractor.HTMLPagesIndexFetcher;
import wdc.productcorpus.datacreator.CorpusExtractor.HTMLPagesIndexofIndexFetcher;


public class Fetcher {

	public static void main(String[] args) {

		Fetcher fetch = new Fetcher();
		JCommander jc = new JCommander(fetch);

		HTMLPagesIndexofIndexFetcher fetchindexOfIndex = new HTMLPagesIndexofIndexFetcher();
		HTMLPagesIndexFetcher fetchIndex = new HTMLPagesIndexFetcher();
		
		jc.addCommand("indexOfindex", fetchindexOfIndex);
		jc.addCommand("index", fetchIndex);


		
		try {

			jc.parse(args);
			switch (jc.getParsedCommand()) {
			case "indexOfindex":
				fetchindexOfIndex.process();
				break;
			case "index":
				fetchIndex.process();
				break;
		
			}

		} catch (Exception pex) {
			if (jc.getParsedCommand() == null) {
				jc.usage();
			} else {
				switch (jc.getParsedCommand()) {
				case "indexOfindex":
					new JCommander(fetchindexOfIndex).usage();
					break;
				case "index":
					new JCommander(fetchIndex).usage();
					break;
				default:
					jc.usage();
				}
			}
		}

	}

}
