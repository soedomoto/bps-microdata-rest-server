package id.go.bps.microdata.service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import id.go.bps.microdata.filter.API;

@API
@Path("api/data")
public class MicrodataService {
	Logger LOG = LoggerFactory.getLogger(MicrodataService.class);
	
	@GET
	public void listDataSource() {
		
	}
	
	public void addDataSource(String ds) {
		
	}
	
	public void editDataSource(String ds) {
		
	}
	
	public void removeDataSource(String ds) {
		
	}
	
	public void listVariable(String ds) {
		
	}
	
	public void addVariable(String ds, String var) {
		
	}
	
	public void editVariable(String ds, String var) {
		
	}
	
	public void removeVariable(String ds, String var) {
		
	}
	
	public void listData(String ds, String var) {
		
	}
	
	public void addData(String ds, String var, String id) {
		
	}
	
	public void editData(String ds, String var, String id) {
		
	}
	
	public void removeData(String ds, String var, String id) {
		
	}
	
}
