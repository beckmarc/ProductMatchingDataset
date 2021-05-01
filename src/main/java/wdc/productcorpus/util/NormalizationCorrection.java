package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.InputUtil;
import ldif.local.datasources.dump.QuadFileLoader;
import ldif.runtime.Quad;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;
/**
 * Takes the normalized offers and replaces the title and information of the non-normalized offers.
 * As for the correction we did not rebuild the dataset but only got the textual attributes we need to add
 * the cluster and normalized identifiers information.
 * This snippet keeps in memory all normalized offers and goes through the non-normalized ones.
 * For every non-normalized offer it replaces the cluster information and the normalized identifiers information
 * @author Anna Primpeli
 *
 */
public class NormalizationCorrection  {
	
	public static void main(String args[]) throws IOException{
		File normalizedOffers = new File(args[0]);
		File nonnormalizedOffers = new File(args[1]);
		File outputFile = new File(args[2]);
		
		NormalizationCorrection correct = new NormalizationCorrection();
		correct.correct(normalizedOffers, nonnormalizedOffers, outputFile);
	}

	private void correct(File normalizedOffers, File nonnormalizedOffers, File outputFile) throws IOException {
		HashMap<String,OutputOffer> n_offers = new HashMap<String,OutputOffer>();
		BufferedReader br = InputUtil.getBufferedReader(normalizedOffers);
		String line ="";
		DataImporter importOffer = new DataImporter();

		while ((line=br.readLine())!=null) {
			JSONObject json = new JSONObject(line);			
			OutputOffer offer = importOffer.jsonToOffer(json);
			n_offers.put(offer.getNodeID()+offer.getUrl(), offer);
		}
		br.close();
		System.out.println("Loaded "+n_offers.size()+"normalized offers");
		
		HashMap<String, OutputOffer> correctedOffers = new HashMap<String,OutputOffer>();
		br = InputUtil.getBufferedReader(nonnormalizedOffers);
		int correctedOffers_counter = 0;
		while ((line=br.readLine())!=null) {
			JSONObject json = new JSONObject(line);			
			OutputOffer offer = importOffer.jsonToOffer(json);
			
			OutputOffer cor_normalized_offer = n_offers.get(offer.getNodeID()+offer.getUrl());
			if (null== cor_normalized_offer) continue;
			//add the needed info 
			offer.setCluster_id(cor_normalized_offer.getCluster_id());
			offer.setIdentifiers(cor_normalized_offer.getIdentifiers());
			correctedOffers_counter++;
			correctedOffers.put(offer.getNodeID()+offer.getUrl(), offer);
			if (correctedOffers_counter%100000==0){
				System.out.println("Wrote "+correctedOffers_counter+" corrected offers.");
				BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile, true));
				for (OutputOffer o: correctedOffers.values()) {
					writer.write(o.toJSONObject(true)+"\n");
				}
				writer.flush();
				writer.close();
				correctedOffers = new HashMap<String,OutputOffer>();
			}
		}
		//write one last time
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile, true));
		for (OutputOffer o: correctedOffers.values()) {
			writer.write(o.toJSONObject(true)+"\n");
		}
		writer.flush();
		writer.close();
		correctedOffers = new HashMap<String,OutputOffer>();
		br.close();
		System.out.println("Corrected "+correctedOffers_counter+" offers");
		System.out.println("DONE");
		

		
	}
}
