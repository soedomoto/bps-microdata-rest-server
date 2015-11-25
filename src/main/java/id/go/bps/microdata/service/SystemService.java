package id.go.bps.microdata.service;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("system")
public class SystemService {
	
	@GET
	@Path("request/ip")
	@Produces(MediaType.TEXT_PLAIN)
	public Response getIp(@Context HttpServletRequest req) {
	    String remoteHost = req.getRemoteHost();
	    String remoteAddr = req.getRemoteAddr();
	    int remotePort = req.getRemotePort();
	    String msg = remoteHost + " (" + remoteAddr + ":" + remotePort + ")";
	    return Response.ok(msg).build();
	}
	
}
