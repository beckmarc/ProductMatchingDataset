package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.zip.GZIPInputStream;

import org.json.JSONObject;

public class CheckForDublicateNodes {

	public static void main (String args[]) throws UnsupportedEncodingException, FileNotFoundException, IOException {
		File filteredOffers = new File(args[0]);
		CheckForDublicateNodes check = new CheckForDublicateNodes();
		check.checkfordubl(filteredOffers);
	}

	private void checkfordubl(File filteredOffers) throws UnsupportedEncodingException, FileNotFoundException, IOException {
		
		HashSet<String> nodeIDs = new HashSet<String>();
		ArrayList<String> dublicateIDs = new ArrayList<String>();
		for (File f : filteredOffers.listFiles()) {
			
			BufferedReader reader = new BufferedReader(new InputStreamReader
					(new GZIPInputStream(new FileInputStream(f)),
					"utf-8"));
			
			String line;
			
			while ((line = reader.readLine()) != null) {
				JSONObject json = new JSONObject(line);
				String urlvalue = json.getString("url");
				String nodevalue = json.getString("nodeID");
				if (nodeIDs.contains(urlvalue+";"+nodevalue)){
					dublicateIDs.add(urlvalue+";"+nodevalue);
					System.out.println(urlvalue+";"+nodevalue);
				}
				nodeIDs.add(urlvalue+";"+nodevalue);
			}
			reader.close();

		}
		
	}
}
