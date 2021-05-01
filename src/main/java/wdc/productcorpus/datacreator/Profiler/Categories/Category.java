package wdc.productcorpus.datacreator.Profiler.Categories;

public class Category {
	private String id;
	private int offerscount=0;
	private int clustersCount=0;
	private int clusters_3_4_count=0;
	private int clusters_5_10_count=0;
	private int clusters_11_20_count=0;
	private int clusters_large_count=0;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public int getOfferscount() {
		return offerscount;
	}
	public void setOfferscount(int offerscount) {
		this.offerscount = offerscount;
	}
	public int getClustersCount() {
		return clustersCount;
	}
	public void setClustersCount(int clustersCount) {
		this.clustersCount = clustersCount;
	}
	public int getClusters_3_4_count() {
		return clusters_3_4_count;
	}
	public void setClusters_3_4_count(int clusters_3_4_count) {
		this.clusters_3_4_count = clusters_3_4_count;
	}
	public int getClusters_5_10_count() {
		return clusters_5_10_count;
	}
	public void setClusters_5_10_count(int clusters_5_10_count) {
		this.clusters_5_10_count = clusters_5_10_count;
	}
	public int getClusters_11_20_count() {
		return clusters_11_20_count;
	}
	public void setClusters_11_20_count(int clusters_11_20_count) {
		this.clusters_11_20_count = clusters_11_20_count;
	}
	public int getClusters_large_count() {
		return clusters_large_count;
	}
	public void setClusters_large_count(int clusters_large_count) {
		this.clusters_large_count = clusters_large_count;
	}
	
	
}
