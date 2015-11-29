package id.go.bps.microdata.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jsoup.helper.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.mapping.CassandraType;

import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;

import id.go.bps.microdata.filter.API;
import id.go.bps.microdata.library.DDIParser;
import id.go.bps.microdata.model.Resource;
import id.go.bps.microdata.model.ResourceFile;
import id.go.bps.microdata.model.ResourceKey;

@API
@Path("api/catalog")
public class CatalogService {
	Logger LOG = LoggerFactory.getLogger(CatalogService.class);
	
	@Autowired
	private CassandraOperations cqlOps;
	
	private StreamingOutput xsltProcessor(final String xslt, final String input, final Map<String, Object> param) {
		StreamingOutput stream = new StreamingOutput() {
			public void write(OutputStream os) {
				try {
					TransformerFactory tFactory = TransformerFactory.newInstance();
					Transformer transformer = tFactory.newTransformer(new StreamSource("xslt/"+ xslt +".xslt"));
					
					if(param != null) {
						Object[] arrParam = param.keySet().toArray();
						for(int p=0; p<param.keySet().size(); p++) {
							transformer.setParameter(String.valueOf(arrParam[p]), param.get(arrParam[p]));
						}
					}
					
					transformer.transform(
						new StreamSource("ddi/" + input + ".xml"), 
						new StreamResult(os)
					);
				} catch (TransformerConfigurationException e) {
					e.printStackTrace();
				} catch (TransformerException e) {
					e.printStackTrace();
				}
			}
		};
		
		return stream;
	}
	
	@GET
	@Path("{name}/{a:info|overview}")
	@Produces({MediaType.APPLICATION_JSON})
	public Response info(@PathParam("name") final String name) {
		try {
			DDIParser parser = new DDIParser(new File("ddi/" + name + ".xml"));
			return Response.ok(parser).build();
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
		
		return Response.ok("Nothing to do").build();
	}
	
	@GET
	@Path("{name}/variables")
	@Produces({MediaType.APPLICATION_JSON})
	public Response variables(@PathParam("name") final String name) {
		try {
			DDIParser parser = new DDIParser(new File("ddi/" + name + ".xml"));
			return Response.ok(parser.getVariables()).build();
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
		
		return Response.ok("Nothing to do").build();
	}
	
	
	
	@GET
	@Path("{name}/accesspolicy")
	@Produces({MediaType.APPLICATION_XML})
	public Response accesspolicy(@PathParam("name") final String name) {
		StreamingOutput stream = xsltProcessor("ddi_accesspolicy", name, null);
		return Response.ok(stream).build();
	}
	
	@GET
	@Path("{name}/sampling")
	@Produces({MediaType.APPLICATION_XML})
	public Response sampling(@PathParam("name") final String name) {
		StreamingOutput stream = xsltProcessor("ddi_sampling", name, null);
		return Response.ok(stream).build();
	}
	
	@GET
	@Path("{name}/{a:questionnaires|questionnaire}")
	@Produces({MediaType.APPLICATION_XML})
	public Response questionnaire(@PathParam("name") final String name) {
		StreamingOutput stream = xsltProcessor("ddi_questionnaires", name, null);
		return Response.ok(stream).build();
	}
	
	@GET
	@Path("{name}/dataprocessing")
	@Produces({MediaType.APPLICATION_XML})
	public Response dataprocessing(@PathParam("name") final String name) {
		StreamingOutput stream = xsltProcessor("ddi_dataprocessing", name, null);
		return Response.ok(stream).build();
	}
	
	@GET
	@Path("{name}/datacollection")
	@Produces({MediaType.APPLICATION_XML})
	public Response datacollection(@PathParam("name") final String name) {
		StreamingOutput stream = xsltProcessor("ddi_datacollection", name, null);
		return Response.ok(stream).build();
	}
	
	@GET
	@Path("{name}/dataappraisal")
	@Produces({MediaType.APPLICATION_XML})
	public Response dataappraisal(@PathParam("name") final String name) {
		StreamingOutput stream = xsltProcessor("ddi_dataappraisal", name, null);
		return Response.ok(stream).build();
	}
	
	@GET
	@Path("{name}/variable/{var}")
	@Produces({MediaType.APPLICATION_XML})
	public Response variable(@PathParam("name") final String name, @PathParam("var") final String var) {
		Map<String, Object> param = new HashMap<String, Object>();
		param.put("search_varID", var);
		
		StreamingOutput stream = xsltProcessor("ddi_variable", name, param);
		return Response.ok(stream).build();
	}
	
	@GET
	@Path("{name}/datafile/{var}/{offset}/{limit}")
	@Produces({MediaType.APPLICATION_XML})
	public Response datafile(@PathParam("name") final String name, @PathParam("var") final String var, 
			@PathParam("offset") final Integer offset, @PathParam("limit") final Integer limit) {
		Map<String, Object> param = new HashMap<String, Object>();
		param.put("file", var);
		param.put("browser_url", "/");
		param.put("page_offset", offset);
		param.put("page_limit", limit);
		
		StreamingOutput stream = xsltProcessor("ddi_datafile", name, param);
		return Response.ok(stream).build();
	}
	
	@GET
	@Path("{name}/{a:data_dictionary|data-dictionary|datafiles}")
	@Produces({MediaType.APPLICATION_XML})
	public Response datafiles(@PathParam("name") final String name) {
		Map<String, Object> param = new HashMap<String, Object>();
		param.put("browser_url", "/");
		
		StreamingOutput stream = xsltProcessor("ddi_datafiles_list", name, param);
		return Response.ok(stream).build();
	}	
	
	@GET
	@Path("{name}")
	@Produces({MediaType.APPLICATION_XML})
	public Response get(@PathParam("name") final String name) {
		StreamingOutput stream = new StreamingOutput() {
			public void write(OutputStream os) {
				try {
					TransformerFactory tFactory = TransformerFactory.newInstance();
					Transformer transformer = tFactory.newTransformer(new StreamSource("xslt/ddi_vargrp_variables.xslt"));
					transformer.setParameter("VarGroupID", "F1");
					transformer.setParameter("browser_url", "00-SP-2010-M1.Nesstar?Index=0&Name=Population");
					
					transformer.transform(
						new StreamSource("ddi/" + name + ".xml"), 
						new StreamResult(os)
					);
				} catch (TransformerConfigurationException e) {
					e.printStackTrace();
				} catch (TransformerException e) {
					e.printStackTrace();
				}
			}
		};
		
		return Response.ok(stream).build();
	}

}
