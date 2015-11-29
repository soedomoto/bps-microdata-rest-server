package id.go.bps.microdata.model;

import java.util.List;

import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table("resource")
public class Resource {
	
	@PrimaryKey
	ResourceKey pk;
	@Column
	String title;
	@Column
	List<String> files;
	@Column
	String lucene;
	
	public Resource() {}
	
	public Resource(ResourceKey pk, String title) {
		super();
		this.pk = pk;
		this.title = title;
	}

	public ResourceKey getPk() {
		return pk;
	}

	public void setPk(ResourceKey pk) {
		this.pk = pk;
	}
	
	public String getTitle() {
		return title;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}

	public List<String> getFiles() {
		return files;
	}

	public void setFile(List<String> files) {
		this.files = files;
	}

}
