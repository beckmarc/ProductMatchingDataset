package wdc.productcorpus.v2.profiler;

import java.util.ArrayList;

public class SchemaOrg {
	
	public SchemaOrg(boolean isType, String key, boolean isPage) {
		super();
		this.isType = isType;
		this.key = key;
		this.isPage = isPage;
	}
	
	public SchemaOrg(String type, String[] properties) {
		super();
		this.properties = properties;
		this.type = type;
	}

	public boolean isType;
	public boolean isPage;
	public String key;
	
	public String type;
	public String[] properties;
	
	
	
}
