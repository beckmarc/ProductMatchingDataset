package wdc.productcorpus.datacreator.Profiler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;



public class IndexProfiler {
	
	File indexFile = new File("C:\\Users\\User\\Desktop\\pages_indexinfo.txt_test");
	File outputDir = new File("C:\\Users\\User\\Desktop\\index");
	
	public static void main(String args[]) {
		
		IndexProfiler profile = new IndexProfiler();
		
		if (args.length>0) {
			profile.indexFile = new File(args[0]);
			profile.outputDir = new File(args[1]);
		}
			
		profile.getIndexToFiles();
	}

	private HashMap<String, HashSet<String>> getIndexToFiles() {
			
		HashMap<String, HashSet<String>> entries = new HashMap<String, HashSet<String>>();
			
			try {

				BufferedReader br = de.dwslab.dwslib.util.io.InputUtil.getBufferedReader(indexFile);
	
				String line;
				long linesCount =0;
				while ((line = br.readLine()) != null) {
					linesCount++;
					
					if (linesCount%1000000==0) System.out.println("Parsed "+linesCount+" lines");
					String lineParts[] = line.split(" ");
					
					String filename = lineParts[17].replaceAll("\"", "").replace(",","").replace("}","");
	
					HashSet<String> existingEntries = entries.get(filename);
					if (null==existingEntries) 
						existingEntries=new HashSet<String>();
					existingEntries.add(line);
					
					entries.put(filename, existingEntries);
					
	 			}
				System.out.println("Lines of indexed items file: "+linesCount);
				System.out.println("Number of files to be parsed: "+entries.size());
				
				System.out.println("Store indices");
				
				long writtenEntries = 0;
				for (Entry<String, HashSet<String>> indexOfFile : entries.entrySet()) {
					BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir+"//"+indexOfFile.getKey().replaceAll("/", "_").replace(".gz", "")));
					
					for (String i:indexOfFile.getValue()) {
						writtenEntries++;
						writer.write(i+"\n");
					}
					writer.flush();
					writer.close();
				}
				
				System.out.println("Written entries: "+writtenEntries);
				br.close();
				return entries;
			}
			catch (Exception e){
				System.out.println(e.getMessage());
				e.printStackTrace();
				return null;
	
			}
		}
}
