package wdc.productcorpus.datacreator.CorpusExtractor;

public class IndexEntry {

	private String url;
	private long offset;
	private long length;
	private String file;
	
	public IndexEntry(String url, long offset, long length, String file) {
		super();
		this.url = url;
		this.offset = offset;
		this.length = length;
		this.file = file;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public long getOffset() {
		return offset;
	}
	public void setOffset(long offset) {
		this.offset = offset;
	}
	public long getLength() {
		return length;
	}
	public void setLength(long length) {
		this.length = length;
	}
	public String getFile() {
		return file;
	}
	public void setFile(String file) {
		this.file = file;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((file == null) ? 0 : file.hashCode());
		result = prime * result + (int) (length ^ (length >>> 32));
		result = prime * result + (int) (offset ^ (offset >>> 32));
		result = prime * result + ((url == null) ? 0 : url.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IndexEntry other = (IndexEntry) obj;
		if (file == null) {
			if (other.file != null)
				return false;
		} else if (!file.equals(other.file))
			return false;
		if (length != other.length)
			return false;
		if (offset != other.offset)
			return false;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		return true;
	}
	
	
}
