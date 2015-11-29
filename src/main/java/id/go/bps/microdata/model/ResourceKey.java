package id.go.bps.microdata.model;

import java.io.Serializable;
import java.util.UUID;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;

@PrimaryKeyClass
public class ResourceKey implements Serializable {
	private static final long serialVersionUID = -373421140738444640L;
	
	@PrimaryKeyColumn(name = "id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	UUID id;
	@PrimaryKeyColumn(name = "ddi_id", ordinal = 1, type = PrimaryKeyType.CLUSTERED)
	String ddiId;
	
	public ResourceKey() {}
	
	public ResourceKey(UUID id, String ddiId) {
		super();
		this.id = id;
		this.ddiId = ddiId;
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

	@Override
	public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((id == null) ? 0 : id.hashCode());
	    result = prime * result + ((ddiId == null) ? 0 : ddiId.hashCode());
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
	    
	    ResourceKey other = (ResourceKey) obj;
	    if (id == null) {
	    	if (other.id != null)
	    		return false;
	    } else if (!id.equals(other.id))
	    	return false;
	    if (ddiId == null) {
	    	if (other.ddiId != null)
	    		return false;
	    } else if (!ddiId.equals(other.ddiId))
	    	return false;
	    return true;
	}

}
