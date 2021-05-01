package wdc.productcorpus.datacreator.CorpusExtractor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DistinctWARCFilenames {

	File inputFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Index\\pages_indexinfo.txt");
	File outputFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\Index\\qfiles.txt");
	
	public static void main(String[] args) throws IOException {

		DistinctWARCFilenames getQFiles = new DistinctWARCFilenames();
		
		if (args.length>0) {
			getQFiles.inputFile = new File(args[0]);
			getQFiles.outputFile = new File(args[1]);
		}
		
		HashSet<String> uniqueWarcs = getQFiles.getUniqueQFiles();
		getQFiles.writeFile(uniqueWarcs);
	}
	
	private void writeFile(HashSet<String> uniqueWarcs) throws IOException {
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
		for (String warc:uniqueWarcs) {
			writer.write(warc+"\n");
		}
		writer.flush();
		writer.close();
		
	}

	public HashSet<String> getUniqueQFiles() throws IOException {
		
		HashSet<String> uniqueWarcs = new HashSet<String>();
		
		BufferedReader reader = new BufferedReader(new FileReader(inputFile));
		String regex = ".*\\\"filename\": \\\"(.*)\\\"\\}";
		Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
		
		String line = "";
		int lineCounter = 0;
		while((line = reader.readLine())!=null) {
			lineCounter++;
			Matcher matcher = pattern.matcher(line);
			while (matcher.find()) {
				uniqueWarcs.add(matcher.group(1));
			}
			
			if (lineCounter%100000==0) System.out.println("Parsed "+lineCounter);
		}
		
		reader.close();
		return uniqueWarcs;
	}

}
