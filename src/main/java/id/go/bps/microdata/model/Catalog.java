package id.go.bps.microdata.model;

import java.util.List;
import java.util.UUID;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;

@Table("catalog")
public class Catalog {
	
	@PrimaryKeyColumn(name = "id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	UUID id;
	@PrimaryKeyColumn(name = "ddi_id", ordinal = 1, type = PrimaryKeyType.CLUSTERED)
	String ddiId;
	@Column
	String title;
	@Column
	List<String> files;
	@Column
	String lucene;
	
	public Catalog() {}

	public Catalog(UUID id, String ddiId, String title, List<String> files) {
		super();
		this.id = id;
		this.ddiId = ddiId;
		this.title = title;
		this.files = files;
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getDdiId() {
		return ddiId;
	}

	public void setDdiId(String ddiId) {
		this.ddiId = ddiId;
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

	public void setFiles(List<String> files) {
		this.files = files;
	}

}
