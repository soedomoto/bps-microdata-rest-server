package id.go.bps.microdata.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;

import id.go.bps.microdata.model.User;
import id.go.bps.microdata.model.UserKey;

@Path("user")
public class UserService {
	Logger LOG = LoggerFactory.getLogger(UserService.class);
	
	@Autowired
	private CassandraOperations cassandraOps;
	
	@GET
	@Path("list")
	@Produces({MediaType.APPLICATION_JSON})
	public List<User> list() {		
		return cassandraOps.select("select * from user", User.class);
	}
	
	@POST
	@Path("add")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces({MediaType.APPLICATION_JSON})
	public Response add(
		@FormParam("firstname") String firstname, 
		@FormParam("lastname") String lastname, 
		@FormParam("username") final String username, 
		@FormParam("password") String password, 
		@FormParam("phones") List<String> phones, 
		@FormParam("emails") List<String> emails, 
		@DefaultValue("false") @FormParam("isadmin") Boolean isAdmin, 
		@DefaultValue("false") @FormParam("isdeveloper") Boolean isDeveloper, 
		@DefaultValue("false") @FormParam("isuser") Boolean isUser
	) {
		if(firstname == null || firstname.isEmpty()) 
			return Response.status(Status.FORBIDDEN).entity(new HashMap<String, Object>() {
				private static final long serialVersionUID = 1L;
			{
				put("error", "'firstname' cannot be null");
			}}).build();
		if(lastname == null || lastname.isEmpty()) 
			return Response.status(Status.FORBIDDEN).entity(new HashMap<String, Object>() {
				private static final long serialVersionUID = 1L;
			{
				put("error", "'lastname' cannot be null");
			}}).build();
		if(username == null || username.isEmpty()) 
			return Response.status(Status.FORBIDDEN).entity(new HashMap<String, Object>() {
				private static final long serialVersionUID = 1L;
			{
				put("error", "'username' cannot be null");
			}}).build();
		if(phones == null || phones.isEmpty()) 
			return Response.status(Status.FORBIDDEN).entity(new HashMap<String, Object>() {
				private static final long serialVersionUID = 1L;
			{
				put("error", "'phones' cannot be null");
			}}).build();
		if(emails == null || emails.isEmpty()) 
			return Response.status(Status.FORBIDDEN).entity(new HashMap<String, Object>() {
				private static final long serialVersionUID = 1L;
			{
				put("error", "'emails' cannot be null");
			}}).build();
		
		Select s = QueryBuilder.select().from("user");
		s.where(QueryBuilder.eq("username", username));
		s.allowFiltering();
		User currUser = cassandraOps.select(s, User.class).get(0);
		if(currUser != null) {
			return Response.status(Status.FORBIDDEN).entity(new HashMap<String, Object>() {
				private static final long serialVersionUID = 1L;
			{
				put("error", "'username' '" + username + "' is not availabe");
			}}).build();
		}
		
		User user = new User();
		user.setPk(new UserKey(UUIDs.timeBased(), username));
		user.setFirstName(firstname);
		user.setLastName(lastname);
		user.setPassword(password);
		user.setCreatedTime(new Date());
		user.getPhones().addAll(phones);
		user.getEmails().addAll(emails);
		user.setAdmin(isAdmin);
		user.setDeveloper(isDeveloper);
		user.setUser(isUser);
		
		User insertedUser = cassandraOps.insert(user);
		return Response.ok(insertedUser).build();
	}
	
	@POST
	@Path("edit")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces({MediaType.APPLICATION_JSON})
	public Response edit(
		@FormParam("firstname") String firstname, 
		@FormParam("lastname") String lastname, 
		@FormParam("username") String username, 
		@FormParam("password") String password, 
		@FormParam("phones") List<String> phones, 
		@FormParam("emails") List<String> emails, 
		@FormParam("isadmin") Boolean isAdmin, 
		@FormParam("isdeveloper") Boolean isDeveloper, 
		@FormParam("isuser") Boolean isUser
	) {
		if(username == null || username.isEmpty()) 
			return Response.status(Status.FORBIDDEN).entity(new HashMap<String, Object>() {
				private static final long serialVersionUID = 1L;
			{
				put("error", "'username' cannot be null");
			}}).build();
		
		Select s = QueryBuilder.select().from("user");
		s.where(QueryBuilder.eq("username", username));
		s.allowFiltering();
		List<User> selUsers = cassandraOps.select(s, User.class);
		if(selUsers.size() > 0) {
			User currUser = cassandraOps.select(s, User.class).get(0);
			
			if(firstname != null && !firstname.isEmpty()) {
				currUser.setFirstName(firstname);
			}
			if(lastname != null && !lastname.isEmpty()) {
				currUser.setLastName(lastname);
			}
			if(password != null && !password.isEmpty()) {
				currUser.setPassword(password);
			}
			if(phones != null && !phones.isEmpty()) {
				List<String> cPhones = new ArrayList<String>();
				cPhones.addAll(currUser.getPhones());
				cPhones.addAll(phones);
				currUser.setPhones(cPhones);
			}
			if(emails != null && !emails.isEmpty()) {
				List<String> cEmails = new ArrayList<String>();
				cEmails.addAll(currUser.getEmails());
				cEmails.addAll(emails);
				currUser.setEmails(cEmails);
			}
			if(isAdmin != null) {
				currUser.setAdmin(isAdmin);
			}
			if(isDeveloper != null) {
				currUser.setDeveloper(isDeveloper);
			}
			if(isUser != null) {
				currUser.setUser(isUser);
			}
			
			try {
				User updUser = cassandraOps.update(currUser);
				return Response.ok(updUser).build();
			} catch(Exception e) {
				LOG.error(e.getMessage());
				return Response.status(Status.NOT_FOUND).entity("Something wrong happened").build();
			}
		}
		
		return Response.ok("No user(s) edited").build();
	}
	
	@POST
	@Path("delete")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces({MediaType.APPLICATION_JSON})
	public Response delete(
		@FormParam("username") String username
	) {
		if(username == null || username.isEmpty()) 
			return Response.status(Status.FORBIDDEN).entity(new HashMap<String, Object>() {
				private static final long serialVersionUID = 1L;
			{
				put("error", "'username' cannot be null");
			}}).build();
		
		Select s = QueryBuilder.select().from("user");
		s.where(QueryBuilder.eq("username", username));
		s.allowFiltering();
		
		List<User> selUsers = cassandraOps.select(s, User.class);
		if(selUsers.size() > 0) {
			User currUser = cassandraOps.select(s, User.class).get(0);
			try {
				cassandraOps.delete(currUser);
				return Response.ok().build();
			} catch(Exception e) {
				LOG.error(e.getMessage());
				return Response.status(Status.NOT_FOUND).entity("Something wrong happened").build();
			}
		}
		
		return Response.ok("No user(s) deleted").build();
	}

}
