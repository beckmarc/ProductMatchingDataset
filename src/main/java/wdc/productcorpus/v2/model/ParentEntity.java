package wdc.productcorpus.v2.model;

import java.util.HashSet;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ParentEntity extends Entity{
	@JsonIgnore
 	public String childNodeId;
 	@JsonIgnore
 	public HashSet<String> inheritChildNodeIds;
}
