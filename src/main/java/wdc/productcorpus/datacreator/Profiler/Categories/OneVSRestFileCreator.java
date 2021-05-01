package wdc.productcorpus.datacreator.Profiler.Categories;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

public class OneVSRestFileCreator {

	HashSet<String> labels = new HashSet<String>();
	File outputPath =new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\fastText");
	File trainingFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\fastText\\trainingDataFiltered");

	
	public static void main (String args[]) throws IOException {
		
		OneVSRestFileCreator createFiles = new OneVSRestFileCreator();
		
		if (args.length>0) {
			createFiles.outputPath=new File(args[0]);
			createFiles.trainingFile = new File(args[1]);
		}
		
		createFiles.getLabels();
		createFiles.createFiles();
	}
	
	public void getLabels() throws IOException{
		
		//parse one throught he file to get all labels
		BufferedReader reader = new BufferedReader(new FileReader(trainingFile));
		
		String line="";
		while ((line=reader.readLine())!=null) {
			String label = line.split("\\t")[0];
			labels.add(label);
		}
		
		reader.close();
	}
	
	public void createFiles() throws IOException{
		
		//create a new training file for every category
		for (String label:labels) {
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputPath.getPath()+"/"+label)));
			
			BufferedReader reader = new BufferedReader(new FileReader(trainingFile));
			
			String line="";
			while ((line=reader.readLine())!=null) {
				if (!line.contains(label)){
					String newLabel = "__label__negative";
					String text = line.split("\\t")[1];
					
					String newLine = newLabel+"\t"+text;
					writer.write(newLine+"\n");					
				}	
				else writer.write(line+"\n");
			}
		
			System.out.println("Created file "+outputPath.getPath()+label);
			reader.close();
			writer.flush();
			writer.close();
		}
	}
	
}
