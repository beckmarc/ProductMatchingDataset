package wdc.productcorpus.v2.model;

import java.util.ArrayList;
import java.util.Arrays;


public class ParseEntityResult {
	
	public ArrayList<Entity> entities;
	public Type resultType;
	
	public static enum Type {
		SEARCHRESULTPAGE,
		ITEMPAGE,
		REALESTATELISTING,
		COLLECTIONPAGE,
		CHECKOUTPAGE
	};
	
	public ParseEntityResult(String[] types) {
//		for(String type: types) {
//			this.types.add(Type.valueOf(type));
//		}
	}
	
	public ParseEntityResult(Type type) {
		this.resultType = type;
	}
	
	public ParseEntityResult() {}
}


