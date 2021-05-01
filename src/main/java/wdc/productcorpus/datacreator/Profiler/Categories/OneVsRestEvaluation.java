package wdc.productcorpus.datacreator.Profiler.Categories;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class OneVsRestEvaluation {

	File inputProbsDir = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\fastText\\binaryProbs");
	
	File GS_labels = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\fastText\\testData_title_desc_spec_tables");
	
	double negativeThreshold = 0.85;
	int offersSize = 10;
	int labelsSize = 26;
	
	public static void main (String args[]) throws IOException {
		
		OneVsRestEvaluation evaluateOneVsRest = new OneVsRestEvaluation();
		FastTextPrediction[][] binaryPredictions = evaluateOneVsRest.combineBinaryPredictions();
		String[] finalPredictions = evaluateOneVsRest.choosePrediction(binaryPredictions);
		String[] gs = evaluateOneVsRest.loadGS();
		evaluateOneVsRest.evaluate(gs, finalPredictions);
	}
	
	public FastTextPrediction[][] combineBinaryPredictions() throws IOException {
		
		FastTextPrediction[][] offerPredictedScores = new FastTextPrediction[offersSize][labelsSize];
		
		for (int model = 0; model< inputProbsDir.listFiles().length;model++) {
			File probsFile = inputProbsDir.listFiles()[model];
			
			BufferedReader reader = new BufferedReader(new FileReader(probsFile));
			String line ="";
			int offerCounter=0;
			while ((line=reader.readLine())!= null){
				String predictedLabel = line.split(" ")[0];
				Double confidence = Double.valueOf(line.split(" ")[1]);
				
				//stupid if the negative class is less than a threshold confident take the other label
				if (confidence<negativeThreshold) {
					predictedLabel = line.split(" ")[2];
					confidence = Double.valueOf(line.split(" ")[3]);
				}
				
				
				FastTextPrediction prediction = new FastTextPrediction();
				prediction.setDomLabel(predictedLabel);
				prediction.setDomConfidence(confidence);
				
				offerPredictedScores[offerCounter][model] = prediction;
				
				offerCounter++;
				if (offerCounter==offersSize) break;
			}
			
			reader.close();
		}
		System.out.println("Loaded Probabilities");
		
		return offerPredictedScores;
	}
	
	
	public String[] choosePrediction(FastTextPrediction[][] binaryPredictions) {
		
		String[] finalPredictions = new String[offersSize];
		
		int offersWithMoreThanOneDominantLabel = 0;
		
		for (int offerit =0; offerit< offersSize; offerit++) {
			String finalPrediction="";
			HashMap<String, Double> uniqueDominantPredictions = new HashMap<>();
			
			for (int modelit=0;modelit<labelsSize;modelit++){
				uniqueDominantPredictions.put(binaryPredictions[offerit][modelit].getDomLabel(), binaryPredictions[offerit][modelit].getDomConfidence());
			}
			if (uniqueDominantPredictions.size()>2) offersWithMoreThanOneDominantLabel++;
			
			uniqueDominantPredictions.remove("__label__negative");
			if (uniqueDominantPredictions.isEmpty()) finalPrediction = "__label__not found";
			else {
				finalPrediction = Collections.max(uniqueDominantPredictions.entrySet(), Comparator.comparingDouble(Map.Entry::getValue)).getKey();
			}
					
			finalPredictions[offerit] = finalPrediction;
		}
		
		System.out.println("Offers with more than one dominant label (excluding the negative class): "+offersWithMoreThanOneDominantLabel);
		return finalPredictions;
	}
	
	
	public String[] loadGS() throws IOException {
		
		BufferedReader reader = new BufferedReader(new FileReader(GS_labels));
		
		String [] correctLabels = new String[offersSize];
		String line ="";
		int offersCount=0;
		while ((line = reader.readLine()) !=null) {
			correctLabels[offersCount] = line.split("\t")[0];
			offersCount++;
			if (offersCount == offersSize) break;
		}
		
		reader.close();
		return correctLabels;
	}
	
	public void evaluate(String[] gs, String[]predictions){
		
		if (gs.length != predictions.length) {
			System.out.println("GS and Predictions length do not match");
			System.exit(1);
		}
		int correct=0;
		int wrong=0;
		for (int i=0;i< offersSize;i++){
			if (gs[i].equals(predictions[i])) correct++;
			else {
				if (gs[i].contains("Clothing"))
					System.out.println("Error line "+(i+1));
				wrong++;
			}
		}
		
		System.out.println("Correct: "+correct+" Wrong:"+wrong+" Total:"+gs.length);
		System.out.println("Precision@1 : "+ ((double) correct/ (double)gs.length));
	}
}
