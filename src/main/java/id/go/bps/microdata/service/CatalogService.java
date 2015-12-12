package id.go.bps.microdata.service;

import java.io.File;
import java.io.FileInputStream;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.Response.Status;
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
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;

import id.go.bps.microdata.filter.API;
import id.go.bps.microdata.library.CassandraUtil;
import id.go.bps.microdata.library.DDIParser;
import id.go.bps.microdata.model.Catalog;
import id.go.bps.microdata.model.Resource;

@API
@Path("api/catalog")
public class CatalogService {
	Logger LOG = LoggerFactory.getLogger(CatalogService.class);
	
	@Autowired
	private CassandraOperations cqlOps;
	
	@POST
	@Path("import/ddi")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.APPLICATION_JSON)
	public Response importDdiFromFile(@FormDataParam("ddifile") final InputStream uplIS,
            @FormDataParam("ddifile") FormDataContentDisposition detail) {
		LOG.info("Received DDI File : " + detail.getFileName());
		
		try {
			new File("ddi/" + detail.getFileName()).getParentFile().mkdirs();
			
			IOUtils.copyLarge(uplIS, new FileOutputStream("ddi/" + detail.getFileName()));			
			DDIParser parser = new DDIParser(new File("ddi/" + detail.getFileName()));
			
			List<CreateTableSpecification> cqlTables = new ArrayList<>();
			List<String> luceneIndices = new ArrayList<>();
			
			String strCql = String.format(
				"SELECT * FROM catalog WHERE lucene = '{ filter : { " + 
					"type  : \"match\", " + 
					"field : \"%s\", " + 
					"value : \"%s\" " + 
				"} }'", 
				"ddi_id", 
				parser.getIDNo()
			);
			Catalog cat = cqlOps.selectOne(strCql, Catalog.class);
			if(cat == null) {
				cat = new Catalog();
				cat.setId(UUIDs.timeBased());
				cat.setDdiId(parser.getIDNo());
				cat.setTitle(parser.getTitle());
				
				List<String> idFiles = new ArrayList<>();
				List<Resource> rFiles = new ArrayList<>();
				for(Map<String, Object> file : parser.getFileDescription()) {
					String tableName = "t" + parser.getIDNo().concat(String.valueOf(file.get("id")))
							.replace("-", "").toLowerCase();
					
					Resource rFile = new Resource(
							UUIDs.timeBased(), 
							String.valueOf(file.get("id")), 
							String.valueOf(file.get("fileName")).replace(".NSDstat", ""), 
							String.valueOf(file.get("fileCont")),
							tableName, 
							cat.getId().toString()
						);
					rFiles.add(rFile);
					idFiles.add(rFile.getId().toString());
					
					//create table
					CreateTableSpecification cqlTable = createTable(parser, String.valueOf(file.get("id")), tableName);
					cqlTables.add(cqlTable);
					
					//create index
					String luceneIndex = createIndex(parser, String.valueOf(file.get("id")), tableName);
					luceneIndices.add(luceneIndex);
				}
				cqlOps.insert(rFiles);
				
				cat.setFiles(idFiles);
				cqlOps.insert(cat);
			} else {
				List<Resource> rFiles = new ArrayList<>();
				List<Map<String, Object>> files = parser.getFileDescription();
				for(String idFile : cat.getFiles()) {
					Select sf = QueryBuilder.select().from("resource_file").allowFiltering();
					sf.where(QueryBuilder.eq("id", UUID.fromString(idFile)));
					Resource rFile = cqlOps.selectOne(sf, Resource.class);
					
					for(Map<String, Object> file : files) {
						if(String.valueOf(file.get("id")).equalsIgnoreCase(rFile.getFileId())) {
							String tableName = "t" + parser.getIDNo().concat(String.valueOf(file.get("id")))
									.replace("-", "").toLowerCase();
							
							rFile.setFileName(String.valueOf(file.get("fileName")).replace(".NSDstat", ""));
							rFile.setFileContent(String.valueOf(file.get("fileCont")));
							rFile.setTableName(tableName);
							
							//create table
							CreateTableSpecification cqlTable = createTable(parser, String.valueOf(file.get("id")), tableName);
							cqlTables.add(cqlTable);
						}
					}
					
					rFiles.add(rFile);
				}
				cqlOps.update(rFiles);
				
				cat.setTitle(parser.getTitle());
				cqlOps.update(cat);
			}
			
			for(CreateTableSpecification cql : cqlTables) cqlOps.execute(cql);
			for(String cql : luceneIndices) cqlOps.execute(cql);
			
			return Response.ok().build();
		} catch (IOException e) {
			LOG.error(e.getMessage());
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error in uploading file(s)").build();
		} catch (DataAccessException e) {
			LOG.error(e.getMessage());
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error in database access").build();
		}
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response catalogs() {
		List<Object> catalogs = new ArrayList<>();
		
		Select s = QueryBuilder.select().from("catalog").allowFiltering();
		for(Catalog res : cqlOps.select(s, Catalog.class)) {
			Map<String, Object> mCats = new HashMap<>();
			mCats.put("id", res.getId().toString());
			mCats.put("ddiId", res.getDdiId());
			mCats.put("title", res.getTitle());
			
			catalogs.add(mCats);
		}
		
		return Response.ok(catalogs).build();
	}
	
	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response catalogById(@PathParam("id") String id) {
		//Select s = QueryBuilder.select().from("resource");
		//s.where(QueryBuilder.eq("lucene", "{ filter : { type : \"all\" } }"));
		
		String cql = String.format(
			"SELECT * FROM catalog WHERE lucene = '{ filter : { " + 
				"type  : \"match\", " + 
				"field : \"%s\", " + 
				"value : \"%s\" " + 
			"} }'", 
			"id", 
			id
		);		
		for(Catalog res : cqlOps.select(cql, Catalog.class)) {
			Map<String, Object> mCats = new HashMap<>();
			mCats.put("id", res.getId().toString());
			mCats.put("ddiId", res.getDdiId());
			mCats.put("title", res.getTitle());
			mCats.put("resources", res.getFiles());
			
			try {
				DDIParser ddi = new DDIParser(new File("ddi/" + res.getDdiId() + ".xml"));
				mCats.put("ddi", ddi);
			} catch (IOException e) {
				LOG.error(e.getMessage());
			}
			
			return Response.ok(mCats).build();
		}
		
		return Response.ok("{}").build();
	}
	
	@GET
	@Path("{id}/thumb")
	@Produces("image/png")
	public Response catalogThumbById(@PathParam("id") String id) {
		//Select s = QueryBuilder.select().from("resource");
		//s.where(QueryBuilder.eq("lucene", "{ filter : { type : \"all\" } }"));
		
		String cql = String.format(
			"SELECT * FROM catalog WHERE lucene = '{ filter : { " + 
				"type  : \"match\", " + 
				"field : \"%s\", " + 
				"value : \"%s\" " + 
			"} }'", 
			"id", 
			id
		);		
		for(Catalog res : cqlOps.select(cql, Catalog.class)) {
			String imgPath = "thumb" + File.separator + res.getDdiId() + ".jpg";
			if(! new File(imgPath).exists()) {
				imgPath = "thumb" + File.separator + "default.png";
			}
			final String img = imgPath;
			return Response.ok(new StreamingOutput() {
				@Override
				public void write(OutputStream output) throws IOException, WebApplicationException {
					IOUtils.copy(new FileInputStream(new File(img)), output);
				}
			}).build();
		}
		
		return Response.ok("{}").build();
	}
	
	private CreateTableSpecification createTable(DDIParser parser, String fileId, String tableName) {
		CreateTableSpecification creTabSpec = CreateTableSpecification
				.createTable(tableName)
				.partitionKeyColumn("id", DataType.varchar());
		
		for(Map<String, Object> var : parser.getVariables()) {
			String file = String.valueOf(var.get("file"));
			String varName = String.valueOf(var.get("name")).toLowerCase();
			
			if(fileId.equalsIgnoreCase(file)) {
				DataType type = DataType.text();
				
				Map<String, Object> valueRange = (Map<String, Object>) var.get("valueRange");
				String varUnit = String.valueOf(valueRange.get("unit"));
				if(varUnit.equalsIgnoreCase("REAL")) {
					type = DataType.cint();
				} else if(var.get("decimal") != null && !String.valueOf(var.get("decimal")).isEmpty() && 
						Integer.valueOf(String.valueOf(var.get("decimal"))) > 0) {
					type = DataType.decimal();
				}
				
				creTabSpec.column(varName, type);
			}
		}
		
		creTabSpec.column("lucene", DataType.text());
		
		return creTabSpec;
	}
	
	private String createIndex(DDIParser parser, String fileId, String tableName) {
		List<String> luceneFields = new ArrayList<>();
		luceneFields.add( "id : {type : \"string\"} ");
		for(Map<String, Object> var : parser.getVariables()) {
			String file = String.valueOf(var.get("file"));
			String varName = String.valueOf(var.get("name")).toLowerCase();
			
			if(fileId.equalsIgnoreCase(file)) {
				String luceneFieldType = "text";
				
				Map<String, Object> valueRange = (Map<String, Object>) var.get("valueRange");
				String varUnit = String.valueOf(valueRange.get("unit"));
				if(varUnit.equalsIgnoreCase("REAL")) {
					luceneFieldType = "integer";
				} else if(var.get("decimal") != null && !String.valueOf(var.get("decimal")).isEmpty() && 
						Integer.valueOf(String.valueOf(var.get("decimal"))) > 0) {
					luceneFieldType = "float";
				}
				
				luceneFields.add(varName + " : { type : \"" + luceneFieldType + "\" }");
			}
		}
		
		String cql = String.format("CREATE CUSTOM INDEX %s_index ON %s (lucene) "
				+ "USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = { "
				+ "'refresh_seconds' : '1', "
				+ "'schema' : '{ fields : { %s } }' };", 
				tableName, tableName, StringUtil.join(luceneFields, " , "));
		
		return cql;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	/*private StreamingOutput xsltProcessor(final String xslt, final String input, final Map<String, Object> param) {
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
	}*/

}
