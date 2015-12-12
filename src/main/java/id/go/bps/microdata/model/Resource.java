package id.go.bps.microdata.model;

import java.util.UUID;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;

@Table("resource")
public class Resource {
	@PrimaryKeyColumn(name = "id", ordinal = 1, type = PrimaryKeyType.CLUSTERED)
	UUID id;
	@PrimaryKeyColumn(name = "file_id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	String fileId;
	@Column("file_name")
	String fileName;
	@Column("file_content")
	String fileContent;
	@Column("table_name")
	String tableName;
	@Column("catalog_id")
	String catalogId;
	@Column
	String lucene;
	
	public Resource() {}

	public Resource(UUID id, String fileId, String fileName, String fileContent, String tableName, String catalogId) {
		super();
		this.id = id;
		this.fileId = fileId;
		this.fileName = fileName;
		this.fileContent = fileContent;
		this.tableName = tableName;
		this.catalogId = catalogId;
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getFileId() {
		return fileId;
	}

	public void setFileId(String fileId) {
		this.fileId = fileId;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFileContent() {
		return fileContent;
	}

	public void setFileContent(String fileContent) {
		this.fileContent = fileContent;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getCatalogId() {
		return catalogId;
	}

	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}
}
