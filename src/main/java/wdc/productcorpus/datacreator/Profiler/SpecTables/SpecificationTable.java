package wdc.productcorpus.datacreator.Profiler.SpecTables;

import org.json.JSONArray;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class SpecificationTable {

	private String content;
	private int keyValuePairsCounter;
	
	private JsonObject keyValuePairs;
	
	
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public int getKeyValuePairsCounter() {
		return keyValuePairsCounter;
	}
	public void setKeyValuePairsCounter(int keyValuePairsCounter) {
		this.keyValuePairsCounter = keyValuePairsCounter;
	}
	public JsonObject getKeyValuePairs() {
		return keyValuePairs;
	}
	public void setKeyValuePairs(JsonObject keyValuePairs) {
		this.keyValuePairs = keyValuePairs;
	}
}
