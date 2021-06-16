package wdc.productcorpus.v2.datacreator.ListingsAds;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;

public class EvaluateClustering {
	static ArrayList<Entity> positives = new ArrayList<Entity>();
	static ArrayList<Entity> negatives = new ArrayList<Entity>();
	static ArrayList<Entity> unclear = new ArrayList<Entity>();
	
	public static void main (String args[]) {
		
		if (args.length!=0) {
			PrintUtils.p(args[0]);
			readLines(new File(args[0]));
		}
		try {
			PrintUtils.p("Positive Pairs: " + positives.size() / 2);
			PrintUtils.p("Negative Pairs: " + negatives.size() / 2);
			PrintUtils.p("Unclear Pairs: " + unclear.size() / 2);
			PrintUtils.p("Correct Pairs: " + (double) positives.size() / (positives.size() + negatives.size() ));
		}
		catch (Exception e){ 
			System.out.println(e.getMessage());
		}
						
		File outputFile = new File(args[1]);
		System.out.println("Writing results to directory: " + outputFile.getPath());
		
		try {
			CustomFileWriter.clearFile("cluster-evaluation.txt", outputFile);
			CustomFileWriter.writeLineToFile("cluster-evaluation", outputFile, "", "---------------------------------------POSITIVES---------------------------------------");
			CustomFileWriter.writeEntitiesToFile("cluster-evaluation", outputFile, "", positives);
			CustomFileWriter.writeLineToFile("cluster-evaluation", outputFile, "", "---------------------------------------Negatives---------------------------------------");
			CustomFileWriter.writeEntitiesToFile("cluster-evaluation", outputFile, "", negatives);
			CustomFileWriter.writeLineToFile("cluster-evaluation", outputFile, "", "---------------------------------------Unclear---------------------------------------");
			CustomFileWriter.writeEntitiesToFile("cluster-evaluation", outputFile, "", unclear);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
		
	private static void readLines(File file) {
		String line;
		try {
			BufferedReader br = InputUtil.getBufferedReader(file);
			while ((line = br.readLine()) != null) {
				if(line.startsWith("true")) {
					positives.add(EntityStatic.parseEntity(line.substring(4)));
				} else if(line.startsWith("false")) {
					negatives.add(EntityStatic.parseEntity(line.substring(5)));
				} else if(line.startsWith("unclear")) {
					unclear.add(EntityStatic.parseEntity(line.substring(7)));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
