package id.go.bps.microdata.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
import com.stratio.cassandra.lucene.builder.Builder;
import com.stratio.cassandra.lucene.builder.search.Search;

import id.go.bps.microdata.model.User;

@Path("api/user")
public class UserService {
	Logger LOG = LoggerFactory.getLogger(UserService.class);
	
	@Autowired
	private CassandraOperations cassandraOps;
	@Autowired 
	@Qualifier("resourceBasedir")
	private String resourceBasedir;
	
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
		@FormParam("phone") String phone, 
		@FormParam("email") String email, 
		@DefaultValue("false") @FormParam("isadmin") Boolean isAdmin, 
		@DefaultValue("false") @FormParam("isdeveloper") Boolean isDeveloper, 
		@DefaultValue("false") @FormParam("isuser") Boolean isUser
	) {
		Map<String, Object> msg = new HashMap<>();
		boolean success = false; 
		
		if(firstname == null || firstname.isEmpty()) 
			msg.put("error", "'firstname' cannot be null");
		if(lastname == null || lastname.isEmpty()) 
			msg.put("error", "'lastname' cannot be null");
		if(username == null || username.isEmpty()) 
			msg.put("error", "'username' cannot be null");
		if(phone == null || phone.isEmpty()) 
			msg.put("error", "'phone' cannot be null");
		if(email == null || email.isEmpty()) 
			msg.put("error", "'email' cannot be null");
		
		Search s = new Search();
		String fil = s.filter(Builder.match("username", username)).build();
		String strCql = String.format(
			"SELECT * FROM %s WHERE lucene = '%s'", 
			"user", fil
		);		
		List<User> users = cassandraOps.select(strCql, User.class);
		
		if(users.size() > 0) {
			msg.put("error", "'username' '" + username + "' is not availabe");
		}
		
		User user = new User();
		user.setUserId(UUIDs.timeBased());
		user.setUserName(username);
		user.setFirstName(firstname);
		user.setLastName(lastname);
		user.setPassword(password);
		user.setCreatedTime(new Date());
		user.getPhones().add(phone);
		user.getEmails().add(email);
		user.setAdmin(isAdmin);
		user.setDeveloper(isDeveloper);
		user.setUser(isUser);
		
		User insertedUser = cassandraOps.insert(user);
		if(insertedUser != null) {
			success = true;
			msg.put("result", insertedUser);
		}
		msg.put("success", success);
		
		return Response.ok(msg).build();
	}
	
	@POST
	@Path("{id}/edit")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces({MediaType.APPLICATION_JSON})
	public Response edit(
		@PathParam("id") String id,
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
		Search s = new Search();
		String fil = s.filter(Builder.match("user_id", id)).build();
		
		String strCql = String.format(
			"SELECT * FROM %s WHERE lucene = '%s'", 
			"user", fil
		);
		
		Map<String, Object> msg = new HashMap<>();
		boolean success = false; 
		
		List<User> users = cassandraOps.select(strCql, User.class);
		if(users.size() > 0) {
			User currUser = users.get(0);
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
				success = true;
				msg.put("result", updUser);
			} catch(Exception e) {
				LOG.error(e.getMessage());
				msg.put("error", "Error in updating user");
			}
		}
		
		msg.put("success", true);
		return Response.ok(msg).build();
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
	
	@POST
	@Path("authenticate")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces({MediaType.APPLICATION_JSON})
	public Response find(@FormParam("username") String username, @FormParam("password") String decPassw) {
		Search s = new Search();
		String fil = s.filter(Builder.bool().must(Builder.match("username", username), 
				Builder.match("password", decPassw))).build();
		String strCql = String.format(
			"SELECT * FROM %s WHERE lucene = '%s'", 
			"user", fil
		);
		
		Map<String, Object> msg = new HashMap<>();
		boolean success = false; 
		
		List<User> users = cassandraOps.select(strCql, User.class);
		if(users.size() > 0) {
			success = true;
			msg.put("result", users.get(0));
		} 
		
		msg.put("success", success);
		return Response.ok(msg).build();
	}
	
	@GET
	@Path("{id}/thumb")
	@Produces("image/jpeg")
	public Response thumb(@PathParam("id") String id) {
		Search s = new Search();
		String fil = s.filter(Builder.match("user_id", id)).build();
		
		String strCql = String.format(
			"SELECT * FROM %s WHERE lucene = '%s'", 
			"user", fil
		);
		
		List<User> users = cassandraOps.select(strCql, User.class);
		if(users.size() > 0) {
			InputStream iis;
			try {
				new File(resourceBasedir + File.separator + "user" + File.separator + "thumb").mkdirs();
				iis = new FileInputStream(resourceBasedir + File.separator + "user" + File.separator + 
						"thumb" + File.separator + users.get(0).getUserId().toString());
			} catch (FileNotFoundException e) {
				iis = UserService.class.getResourceAsStream("anonymous.png");
			}
			
			final InputStream tis = iis;
			return Response.ok(new StreamingOutput() {
				@Override
				public void write(OutputStream output) throws IOException, WebApplicationException {
					IOUtils.copy(tis, output);
				}
			}).build();
		} 
		
		return Response.ok().build();
	}
	
	@POST
	@Path("{id}/thumb/update")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces("image/jpeg")
	public Response importDdiFromFile(
		@PathParam("id") String id, 
		@FormDataParam("accountthumb") final InputStream uplIS,
        @FormDataParam("accountthumb") FormDataContentDisposition detail
	) {
		LOG.info("Received Profle Image : " + detail.getFileName());
		
		try {
			IOUtils.copy(
				uplIS, 
				new FileOutputStream(resourceBasedir + File.separator + "user" + 
						File.separator + "thumb" + File.separator + id)
			);
			
			return Response.ok(new StreamingOutput() {
				@Override
				public void write(OutputStream output) throws IOException, WebApplicationException {
					IOUtils.copy(uplIS, output);
				}
			}).build();
		} catch (IOException e) {
			LOG.error(e.getMessage());
			return Response.ok(UserService.class.getResourceAsStream("anonymous.png")).build();
		}
	}

}
