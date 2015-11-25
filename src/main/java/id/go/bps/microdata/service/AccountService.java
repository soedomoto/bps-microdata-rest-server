package id.go.bps.microdata.service;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import id.go.bps.microdata.model.User;

@Path("account")
public class AccountService {
	
	Logger LOG = LoggerFactory.getLogger(AccountService.class);
	
	@Autowired
	private CassandraOperations cassandraOps;
	
	@GET
	@Path("login/{username}/{password}")
	@Produces({MediaType.APPLICATION_JSON})
	public String login(@PathParam("username") String username, @PathParam("password") String password, 
			@Context HttpServletRequest request) {
		
		Select s = QueryBuilder.select().from("user");
		s.where(QueryBuilder.eq("username", username));
		s.allowFiltering();
		
		User authUser = cassandraOps.selectOne(s, User.class);
		if(authUser != null && authUser.getPassword().equals(password)) {
			request.getSession().setAttribute("authuser", authUser);
			return "Welcome " + authUser.getFirstName() + " " + authUser.getLastName();
		} else {
			return "Invalid username or password";
		}
	}
	
	@GET
	@Path("info")
	@Produces({MediaType.APPLICATION_JSON})
	public Object info(@Context HttpServletRequest request) {
		return request.getSession().getAttribute("authuser");
	}
	
	@GET
	@Path("logout")
	@Produces({MediaType.APPLICATION_JSON})
	public String logout(@Context HttpServletRequest request) {
		request.getSession().removeAttribute("authuser");
		return "You are logged out";
	}
	
}
