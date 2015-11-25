package id.go.bps.microdata.service;

import java.security.Key;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import id.go.bps.microdata.model.User;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;

@Path("token")
public class TokenService {
	
	@Autowired
	@Qualifier("payload")
	private String payload;
	@Autowired
	@Qualifier("rsaKey")
	private Key rsaKey;	
	@Autowired
	@Qualifier("generatedRsaKey")
	private Key generatedRsaKey;
	
	@GET
	@Path("generate")
	@Produces({MediaType.APPLICATION_JSON})
	public Response generate(@Context HttpServletRequest request) {
		if(request.getSession().getAttribute("authuser") == null) {
			return Response.status(401).entity("You have to login first").build();
		}
		
		try {
			Date exp = new Date(System.currentTimeMillis() + (1000 * 60 * 60 * 24 * 7));
			SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
			
			String token = Jwts.builder()
					.setAudience(((User) request.getSession().getAttribute("authuser")).getPk().getUserId().toString())
					.setExpiration(exp)
					.signWith(SignatureAlgorithm.HS512, rsaKey)
					.compact();
			
			Map<String, Object> jwt = new HashMap<>();
			jwt.put("token", token);
			jwt.put("expires", dt.format(exp));
			return Response.ok(jwt).build();
		} catch(Exception e) {
			return Response.status(500).entity("Error in generating access token").build();
		}
	}
	
	@GET
	@Path("verify/{token}")
	@Produces({MediaType.APPLICATION_JSON})
	public Response verify(@PathParam("token") String token) {
		try {
			Claims payload = Jwts.parser().setSigningKey(rsaKey).parseClaimsJws(token).getBody();
			if(payload.getExpiration().after(new Date())) {
				return Response.status(202).entity("Access token is still valid").build();
			} else {
				return Response.status(401).entity("Expired access token").build();
			}
		} catch(SignatureException e) {
			return Response.status(401).entity(e.getMessage()).build();
		} catch(Exception e) {
			return Response.status(500).entity("Something wrong happened").build();
		}
	}
	
	@GET
	@Path("info/{token}")
	@Produces({MediaType.APPLICATION_JSON})
	public Response info(@PathParam("token") String token) {
		try {
			Claims payload = Jwts.parser().setSigningKey(rsaKey).parseClaimsJws(token).getBody();
			return Response.ok(payload).build();
		} catch(SignatureException e) {
			return Response.status(401).entity(e.getMessage()).build();
		} catch(Exception e) {
			return Response.status(500).entity("Something wrong happened").build();
		}
	}
	
}
