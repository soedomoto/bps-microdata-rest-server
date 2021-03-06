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
	@Column("data_kind")
	String dataKind;
	@Column("abstract")
	String abstrakt;
	//@Column("ddi_file")
	//ByteBuffer ddiFile;
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

	public List<String> getResources() {
		return files;
	}

	public void setResources(List<String> files) {
		this.files = files;
	}

	public String getDataKind() {
		return dataKind;
	}

	public void setDataKind(String dataKind) {
		this.dataKind = dataKind;
	}

	public String getAbstract() {
		return abstrakt;
	}

	public void setAbstract(String abstrakt) {
		this.abstrakt = abstrakt;
	}
	
	/*public ByteBuffer getDdiFile() {
		return ddiFile;
	}

	public void setDdiFile(ByteBuffer ddiFile) {
		this.ddiFile = ddiFile;
	}*/

}
