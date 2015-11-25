package id.go.bps.microdata.filter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.Key;
import java.util.Date;
import java.util.Enumeration;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.message.internal.MediaTypes;
import org.glassfish.jersey.server.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import id.go.bps.microdata.model.User;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureException;

public class APIFilter implements ContainerRequestFilter {
	Logger LOG = LoggerFactory.getLogger(APIFilter.class);
	
	@Autowired
	@Qualifier("rsaKey")
	private Key rsaKey;	
	@Context
    HttpServletRequest request;
	@Autowired
	private CassandraOperations cassandraOps;

	@Override
	public void filter(ContainerRequestContext context) throws IOException {
		String token = null;
		// If via header
		if(context.getHeaderString("x-access-token") != null) {
			token = context.getHeaderString("x-access-token");
		} 
		else {
			// If via get
			if(context.getMethod().equalsIgnoreCase("get")) {
				token = context.getUriInfo().getQueryParameters(true).getFirst("token");
				if(token == null) {
					token = context.getUriInfo().getPathParameters(true).getFirst("token");
				}
			}
			// If via post
			else if(context.getMethod().equalsIgnoreCase("post")) {
				// If multipart
				((ContainerRequest) context).bufferEntity();
				if(((ContainerRequest) context).hasEntity() && MediaTypes.typeEqual(MediaType.MULTIPART_FORM_DATA_TYPE, context.getMediaType())) {
					FormDataMultiPart multiPart = ((ContainerRequest) context).readEntity(FormDataMultiPart.class);
					token = multiPart.getField("token").getValue();
				} 
				else if(((ContainerRequest) context).hasEntity() && MediaTypes.typeEqual(MediaType.APPLICATION_FORM_URLENCODED_TYPE, context.getMediaType())) {
					Form multiPart = ((ContainerRequest) context).readEntity(Form.class);
					token = multiPart.asMap().getFirst("token");
				} 
			}
		}
		
		// get request address
		String reqIP = request.getRemoteAddr();
		boolean sameOrigin = false;
		
		// get localhost
		Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
		while(en.hasMoreElements()){
			NetworkInterface ni = (NetworkInterface) en.nextElement();
			Enumeration<InetAddress> ee = ni.getInetAddresses();
			while(ee.hasMoreElements()) {
		        InetAddress ia = (InetAddress) ee.nextElement();
		        if(ia.getHostAddress().equalsIgnoreCase(reqIP)) {
		        	sameOrigin = true;
		        	break;
		        }
		    }
		}
		
		//if local -> allow
		if(! sameOrigin) {
			User authUser = null;
			if(token != null) {
				try {
					Claims payload = Jwts.parser().setSigningKey(rsaKey).parseClaimsJws(token).getBody();
					if(payload.getExpiration().after(new Date())) {
						String user = payload.getAudience();
						Select s = QueryBuilder.select().from("user");
						s.where(QueryBuilder.eq("user_id", UUID.fromString(user)));
						authUser = cassandraOps.selectOne(s, User.class);
					} else {
						context.abortWith(Response.status(401).entity("Expired access token").build());
					}
				} catch(SignatureException e) {
					context.abortWith(Response.status(401).entity("Invalid access token").build());
				} catch(Exception e) {
					LOG.warn(e.getMessage());
					context.abortWith(Response.status(401).entity("Something wrong with access token").build());
				}
			} 
			else if(request.getSession().getAttribute("authuser") != null) {
				LOG.info(String.valueOf(request.getSession().getAttribute("authuser") != null));
				authUser = (User) request.getSession().getAttribute("authuser");
			}
			else {
				context.abortWith(Response.status(401).entity("No token is provided").build());
			}
			
			if(authUser == null) 
				context.abortWith(Response.status(401).entity("User assigned to token is not found").build());
		}
	}

}
