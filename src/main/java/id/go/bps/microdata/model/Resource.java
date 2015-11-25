package id.go.bps.microdata.model;

import java.util.UUID;

import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table("resource")
public class Resource {
	
	@PrimaryKey
	UUID id;
	@Column("ddi_id")
	String ddiId;
	@Column
	String title;
	@Column("table_name")
	String tableName;
	
	public Resource() {
		
	}
	
	public Resource(UUID id, String ddiId, String title, String tableName) {
		super();
		this.id = id;
		this.ddiId = ddiId;
		this.title = title;
		this.tableName = tableName;
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
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

}
