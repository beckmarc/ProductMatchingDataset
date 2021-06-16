package wdc.productcorpus.v2.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

import wdc.productcorpus.datacreator.Extractor.SupervisedNode;
import wdc.productcorpus.v2.model.Entity;

public class CustomFileWriter {
	
	public static void clearFile(String fileName, File directory) {	
		PrintWriter writer;
		try {
			writer = new PrintWriter(directory.toString() + "/" + fileName);
			writer.print("");
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}

	public static void writeTextToFile(String fileName, File outputDirectory, String operationName, ArrayList<String> data) throws IOException {
		
		if(!operationName.equals("")) {
			operationName = "-" + operationName;
		}
		String fullFileName = outputDirectory.toString() + "/" + fileName + operationName + ".txt";
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(fullFileName,true));
		
		for (String line:data)
			writer.write(line+"\n");
		
		writer.flush();
		writer.close();
		
		PrintUtils.p("Wrote output to File:" + fullFileName);
	}
	
	public static void writeTextToFile(String fileName, File outputDirectory, String operationName, Set<String> data) throws IOException {
			
		if(!operationName.equals("")) {
			operationName = "-" + operationName;
		}
		String fullFileName = outputDirectory.toString() + "/" + fileName + operationName + ".txt";
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(fullFileName,true));
		
		for (String line:data)
			writer.write(line+"\n");
		
		writer.flush();
		writer.close();
		
		PrintUtils.p("Wrote output to File:" + fullFileName);
	}
	
	public static void writeLineToFile(String fileName, File outputDirectory, String operationName, String line) throws IOException {
		
		if(!operationName.equals("")) {
			operationName = "-" + operationName;
		}
		String fullFileName = outputDirectory.toString() + "/" + fileName + operationName + ".txt";
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(fullFileName,true));
		
		writer.write(line+"\n");
		writer.flush();
		writer.close();
	}
	
	public static void writeLineToFile(String fileName, File outputDirectory, String line) {

		try {
			String fullFileName = outputDirectory.toString() + "/" + fileName ;
			
			BufferedWriter writer = new BufferedWriter (new FileWriter(fullFileName,true));
			
			writer.write(line+"\n");
			writer.flush();
			writer.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
	public static void writeNodesToFile(String fileName, File outputDirectory, String operationName, ArrayList<SupervisedNode> data) throws IOException {
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString() + "/" + fileName + "-" + operationName + ".txt",true));
		
		for (SupervisedNode node:data)
			writer.write(node.nodetoString()+"\n");
		
		writer.flush();
		writer.close();
		
	}
	
	public static void writeEntitiesToFile(String fileName, File outputDirectory, String operationName, ArrayList<Entity> data) throws IOException {
		
		if(!operationName.equals("")) {
			operationName = "-" + operationName;
		}
		String fullFileName = outputDirectory.toString() + "/" + fileName + operationName + ".txt";
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(fullFileName,true));
		
		ObjectMapper objectMapper = new ObjectMapper();
		
		for (Entity node:data)
			writer.write(objectMapper.writeValueAsString(node)+"\n");
		
		writer.flush();
		writer.close();
	}
	
	
	public static void writePairsToFile(String fileName, File outputDirectory, ArrayList<Entity> data) throws IOException {
		
		String fullFileName = outputDirectory.toString() + "/" + fileName;
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(fullFileName,true));
		
		ObjectMapper mapper = new ObjectMapper();
		
		for(Entity e : data) {
			writer.write(mapper.writeValueAsString(e)); // writerWithDefaultPrettyPrinter()
			writer.write("\n");
		}
		
		
		writer.flush();
		writer.close();
	}
	
	public static void writeEntitiesToFile(File outputFilePath, ArrayList<Entity> data, ObjectMapper objectMapper) throws IOException {
				
		
	}
	
	
	/**
	 * Removes .gz or .txt extensions of a string.<br> 
	 * 
	 * Intended to remove the extension of a fileName. <br>
	 * e.g. example.txt => example
	 * 
	 * @param fileName
	 * @return
	 */
	public static String removeExt(String fileName) {
		if(fileName.matches(".+\\.gz")) {
			return fileName.substring(0, fileName.length()-3);
		}
		else if (fileName.matches(".+\\.txt")) {
			return fileName.substring(0, fileName.length()-4); // removes .txt
		}
		else {
			return fileName;
		}
		
	}
	
	public static <K, V> void writeFormattedKeyValuesToFile(String fileName, File outputDirectory, Map<K, V> map) {
		
		String fullFileName = outputDirectory.toString() + "/" + fileName;	
		
		try {
			BufferedWriter writer = new BufferedWriter (new FileWriter(fullFileName,true));	
			for(Map.Entry<K, V> entry : map.entrySet()) {
				writer.write(entry.getKey() + " : " +  String.format(Locale.US, "%,d", entry.getValue()) + "\n");
			}
			
			writer.flush();
			writer.close();
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}		
	
	}

	public static <K, V> void writeKeyValuesToFile(String fileName, File outputDirectory, Map<K, V> map) {
		
		String fullFileName = outputDirectory.toString() + "/" + fileName;	
		
		try {
			BufferedWriter writer = new BufferedWriter (new FileWriter(fullFileName,true));	
			for(Map.Entry<K, V> entry : map.entrySet()) {
				writer.write(entry.getKey() + " : " +  entry.getValue() + "\n");
			}
			
			writer.flush();
			writer.close();
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}		
	
	}

	public static void writePairsToFile(String fileName, File outputDirectory, String string2,
			Map<Integer, ArrayList<Entity>> data) {	
		try {
			String fullFileName = outputDirectory.toString() + "/" + fileName;
			
			BufferedWriter writer = new BufferedWriter (new FileWriter(fullFileName,true));
			
			ObjectMapper mapper = new ObjectMapper();
			
			for(ArrayList<Entity> ae : data.values()) {
				for(Entity e: ae) {
					writer.write(mapper.writeValueAsString(e)); // writerWithDefaultPrettyPrinter()
					writer.write("\n");
				}
				
			}
		
			writer.flush();
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
