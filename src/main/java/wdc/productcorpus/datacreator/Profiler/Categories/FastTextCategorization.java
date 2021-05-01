package wdc.productcorpus.datacreator.Profiler.Categories;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import wdc.productcorpus.util.StDevStats;

/**
 * @author Anna Primpeli
 * We use an external evaluator for fast text in order to be able to predict the negative class as well using the predicted probabilities of the labels
 * Best config:title_desc_tables, threshold 0.00, dominant 1*1.2, wordLimit 2 
 */
public class FastTextCategorization {

	String labelPrefix = "__label__";
	File fastTextprobs = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\fastText\\predictedProbs_title_desc_tables");
	double threshold = 0.00;
	int dominantCategoryShouldBeBiggerThan = 1;
	int wordLimit =2;
	File outputFinalPredictions = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\fastText\\predictedLabels_title_desc_tables");
	
	File goldStandard = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\fastText\\gsFasttext_title_desc_tables");
	
	public static void main (String args[]) {
		FastTextCategorization categorize = new FastTextCategorization();
		
		try {
			//store probs
			ArrayList<HashMap<String,Float>> offerPredictions = categorize.storeProbs();
			
			//predict (also negative class)
			ArrayList<String> predictedLabels = categorize.predictedLabels(offerPredictions);
			
			//store final predictions
			categorize.storePredictions(predictedLabels);
			
			//evaluate
			categorize.evaluate(predictedLabels);
			
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	
	public void evaluate(ArrayList<String> predictedLabels) throws IOException {
		
		ArrayList<String> gsLabels = new ArrayList<>();
		//read labels from gs - the order is the same
		BufferedReader reader = new BufferedReader(new FileReader(goldStandard));
		
		String line ="";
		while ((line = reader.readLine()) !=null) {
			gsLabels.add(line.split("\t")[0]);
		}
		
		reader.close();
		
		if (gsLabels.size() != predictedLabels.size()) {
			System.out.println("[FastTextCategorization] Sth is wrong. the predictions and the gs are of different sizes. Exit.");
			System.exit(1);
		}
		//and now evaluate - precision @1 = accuracy
		int correct =0;
		int notFoundIncorrect = 0;
		for(int i=0; i<gsLabels.size();i++) {
			if (gsLabels.get(i).equals(predictedLabels.get(i))) correct++;
			else {
				//System.out.println("ERROR: "+(i+1));
				if (gsLabels.get(i).contains("not found")) {
					notFoundIncorrect++;
					System.out.println("ERROR: "+(i+1));
				}
			}
			
		}
		
		System.out.println("Not found incorrect:"+notFoundIncorrect);
		System.out.println("Correct Labels: "+correct);
		System.out.println("All Labels: "+gsLabels.size());

		System.out.println("Accuracy after considering negative class: "+ ((double) correct)/((double) gsLabels.size()));
		
	}
	
	public void storePredictions(ArrayList<String> predictedLabels) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputFinalPredictions));
		
		for(String label: predictedLabels) {
			writer.write(label+"\n");
		}
		
		writer.flush();
		writer.close();
	}
	
	public ArrayList<String> predictedLabels (ArrayList<HashMap<String,Float>> offerPredictions) throws IOException {
				
		int falsePositives=0;
		int falseNegatives = 0;
		
		ArrayList<String> gsLabels = new ArrayList<>();
		ArrayList<Integer> wordCount = new ArrayList<>();
		//read labels from gs - the order is the same
		BufferedReader reader = new BufferedReader(new FileReader(goldStandard));
		
		String line ="";
		while ((line = reader.readLine()) !=null) {
			gsLabels.add(line.split("\t")[0]);
			if (line.split("\t").length==1) wordCount.add(0);
			else 
				wordCount.add(line.split("\t")[1].split(" ").length);
		}
		
		reader.close();
		
		
		StDevStats stats = new StDevStats();
		
		ArrayList<String> predictedLabels = new ArrayList<String>();
		
		for (int i=0; i<offerPredictions.size(); i++){
			double probs [] = new double[26];
			double max= 0;
			String maxLabel = "";
			int pos=0;
			
			ArrayList<Float> predictionsProbs = new ArrayList<Float>();

			for (Map.Entry<String, Float> v: offerPredictions.get(i).entrySet()) {
				
				predictionsProbs.add(v.getValue());

				if (v.getValue()>max) {
					max = v.getValue();
					maxLabel = v.getKey();
				}
				probs[pos] = v.getValue();
				pos++;
			}
		//	double stdev = stats.getStdDev(probs);
			
			
//			if (gsLabels.get(i).contains("not found") && max>threshold) {
//				falsePositives++;
//				System.out.println((i+1)+" False Positive:"+max);
//				}
//			if (!gsLabels.get(i).contains("not found") && max<threshold) {
//				falseNegatives++;
//				System.out.println((i+1)+"False Negative:"+max);
//			}

			// if below a certain threshold assign directly not found
			if (max < threshold) { // assign the non found label
				predictedLabels.add(labelPrefix+"not found");
			}
			//if less that x words assign not found
			else if (wordCount.get(i)< wordLimit)
				predictedLabels.add(labelPrefix+"not found");

			else { //consider the sum of the x next predictions in comparison to the top prediction
				Collections.sort(predictionsProbs);
				float topScore = predictionsProbs.get(predictionsProbs.size()-1);
				
				float sumScore = 0;
				for (int p=predictionsProbs.size()-2; p>=(predictionsProbs.size()-1-dominantCategoryShouldBeBiggerThan); p--) {
					sumScore += predictionsProbs.get(p);
				}
				
				//if the dominant category prediction prob does not surpass the sum of other assign not found
				if (topScore < 1.2*sumScore) predictedLabels.add(labelPrefix+"not found");
				else predictedLabels.add(maxLabel);
			}
			
		}
		
//		System.out.println("False Positives :"+falsePositives);
//		System.out.println("False Negatives :"+falseNegatives);
		return predictedLabels;
	}
	
 	public ArrayList<HashMap<String,Float>> storeProbs() throws IOException {
		
		ArrayList<HashMap<String,Float>> offerPredictions = new ArrayList<>();
		
		BufferedReader reader = new BufferedReader(new FileReader(fastTextprobs));
		
		String line = "";
		while ((line = reader.readLine()) != null) {
			
			HashMap<String,Float> predictionsForOffer = new HashMap<>();
			
			String lineparts[] = line.split(" ");

			for (int i=0; i<52; i=i+2) {
				predictionsForOffer.put(lineparts[i], Float.valueOf(lineparts[i+1]));
				
			}
			offerPredictions.add(predictionsForOffer);
		}
		
		reader.close();
		
		return offerPredictions;
	}
}
