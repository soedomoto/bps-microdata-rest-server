package id.go.bps.microdata.model;

import java.io.Serializable;
import java.util.UUID;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;

@PrimaryKeyClass
public class UserKey implements Serializable {
	private static final long serialVersionUID = -944276954356288826L;
	
	@PrimaryKeyColumn(name = "user_id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	private UUID userId;
	@PrimaryKeyColumn(name = "username", ordinal = 1, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING)
	private String userName;
	
	public UserKey() {
		
	}
	
	public UserKey(UUID userId, String userName) {
		super();
		this.userId = userId;
		this.userName = userName;
	}

	public UUID getUserId() {
		return userId;
	}

	public void setUserId(UUID userId) {
		this.userId = userId;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}
	
	@Override
	public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((userName == null) ? 0 : userName.hashCode());
	    result = prime * result + ((userId == null) ? 0 : userId.hashCode());
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
	    
	    UserKey other = (UserKey) obj;
	    if (userName == null) {
	    	if (other.userName != null)
	    		return false;
	    } else if (!userName.equals(other.userName))
	    	return false;
	    if (userId == null) {
	    	if (other.userId != null)
	    		return false;
	    } else if (!userId.equals(other.userId))
	    	return false;
	    return true;
	}

}
