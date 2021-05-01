package wdc.productcorpus.datacreator.Profiler.Categories;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import de.dwslab.dwslib.util.io.InputUtil;

public class LexiconsImporter {

	private  File lexiconsDir = new File ("");
	
	public LexiconsImporter (File lexicon) {
		this.lexiconsDir = lexicon;
	}
	
	public ArrayList<CategoryScoring> saveCategoryScoringInfo(boolean rescale) throws IOException {
		
		File[] categoryLexicons = lexiconsDir.listFiles();
		
		ArrayList<CategoryScoring> categoriesScoring = new ArrayList<CategoryScoring>();
		
		for (int i=0; i<categoryLexicons.length;i++ ) {
			BufferedReader reader = InputUtil.getBufferedReader(categoryLexicons[i]);
		
			String categoryName = categoryLexicons[i].getName().replace(".txt", "");
			CategoryScoring cs = new CategoryScoring();
			cs.setCategory_name(categoryName);
			
			String line="";
			
			//rescale
			double sum= 0.0;
			
			while (reader.ready()) {
				line = reader.readLine();
				String word = line.split("\\t")[0];
				Double tfidf = Double.valueOf(line.split("\\t")[1]);
				cs.getWordScores().put(word, tfidf);
				sum += tfidf;
			}
			
			if (rescale) {
				if (sum != 1.0){
					System.out.println("Rescale values for category "+ cs.getCategory_name());
					for (String word: cs.getWordScores().keySet()){
						double currentScore = cs.getWordScores().get(word);
						cs.getWordScores().put(word, currentScore/sum);
					}
						
				}
			}
			
			
			categoriesScoring.add(cs);
		}
		return categoriesScoring;
	}
}
