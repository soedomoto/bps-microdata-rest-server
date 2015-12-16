package id.go.bps.microdata.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;

@Table("user")
public class User {
	
	@PrimaryKeyColumn(name = "user_id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	private UUID userId;
	@PrimaryKeyColumn(name = "username", ordinal = 1, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING)
	private String userName;
	@Column("firstname")
	String firstName;
	@Column("lastname")
	String lastName;
	@Column
	String password;
	@Column
	List<String> phones = new ArrayList<>();
	@Column
	List<String> emails = new ArrayList<>();
	@Column("isadmin")
	boolean isAdmin;
	@Column("isdeveloper")
	boolean isDeveloper;
	@Column("isuser")
	boolean isUser;
	@Column("created_time")
	Date createdTime;
	@Column
	String lucene;
	
	public User() {
		
	}

	public User(UUID userId, String userName, String firstName, String lastName, String password, List<String> phones,
			List<String> emails, boolean isAdmin, boolean isDeveloper, boolean isUser, Date createdTime) {
		super();
		this.userId = userId;
		this.userName = userName;
		this.firstName = firstName;
		this.lastName = lastName;
		this.password = password;
		this.phones = phones;
		this.emails = emails;
		this.isAdmin = isAdmin;
		this.isDeveloper = isDeveloper;
		this.isUser = isUser;
		this.createdTime = createdTime;
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

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public List<String> getPhones() {
		return phones;
	}

	public void setPhones(List<String> phones) {
		this.phones = phones;
	}

	public List<String> getEmails() {
		return emails;
	}

	public void setEmails(List<String> emails) {
		this.emails = emails;
	}

	public boolean isAdmin() {
		return isAdmin;
	}

	public void setAdmin(boolean isAdmin) {
		this.isAdmin = isAdmin;
	}

	public boolean isDeveloper() {
		return isDeveloper;
	}

	public void setDeveloper(boolean isDeveloper) {
		this.isDeveloper = isDeveloper;
	}

	public boolean isUser() {
		return isUser;
	}

	public void setUser(boolean isUser) {
		this.isUser = isUser;
	}

	public Date getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(Date createdTime) {
		this.createdTime = createdTime;
	}
	
}
