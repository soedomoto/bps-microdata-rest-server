package id.go.bps.microdata.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jsoup.helper.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.utils.UUIDs;
import com.stratio.cassandra.lucene.builder.Builder;
import com.stratio.cassandra.lucene.builder.search.Search;
import com.stratio.cassandra.lucene.builder.search.condition.Condition;

import id.go.bps.microdata.filter.API;
import id.go.bps.microdata.library.DDIParser;
import id.go.bps.microdata.model.Catalog;
import id.go.bps.microdata.model.Resource;

@API
@Path("api/catalog")
public class CatalogService {
	Logger LOG = LoggerFactory.getLogger(CatalogService.class);
	
	@Autowired
	private CassandraOperations cqlOps;
	@Autowired 
	@Qualifier("resourceBasedir")
	private String resourceBasedir;
	
	@POST
	@Path("import/ddi")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.APPLICATION_JSON)
	public Response importDdiFromFile(@FormDataParam("ddifile") final InputStream uplIS,
            @FormDataParam("ddifile") FormDataContentDisposition detail) {
		LOG.info("Received DDI File : " + detail.getFileName());
		
		try {
			String fileOut = resourceBasedir + File.separator + "ddi" + File.separator + detail.getFileName();
			new File(fileOut).getParentFile().mkdirs();
			
			IOUtils.copyLarge(uplIS, new FileOutputStream(fileOut));			
			DDIParser parser = new DDIParser(new File(fileOut));
			
			List<CreateTableSpecification> cqlTables = new ArrayList<>();
			List<String> luceneIndices = new ArrayList<>();
			
			String filter = new Search().filter(Builder.match("ddi_id", parser.getIDNo())).build();
			Catalog cat = cqlOps.selectOne(
				String.format("SELECT * FROM catalog WHERE lucene = '%s'", filter), 
				Catalog.class
			);
			if(cat == null) {
				cat = new Catalog();
				cat.setId(UUIDs.timeBased());
				cat.setDdiId(parser.getIDNo());
				cat.setTitle(parser.getTitle());
				cat.setDataKind(parser.getDataKind());
				cat.setAbstract(parser.getAbstract());
				
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
				
				cat.setResources(idFiles);
				cqlOps.insert(cat);
			} else {
				List<Resource> rFiles = new ArrayList<>();
				List<Map<String, Object>> files = parser.getFileDescription();
				for(String idFile : cat.getResources()) {
					String fil = new Search().filter(Builder.match("id", UUID.fromString(idFile))).build();
					Resource rFile = cqlOps.selectOne(
						String.format("SELECT * FROM resource WHERE lucene='%s'", fil), 
						Resource.class
					);
					
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
		List<Catalog> cats = cqlOps.select(
			String.format("SELECT * FROM catalog WHERE lucene = '%s'", 
				new Search().filter(Condition.all())), 
			Catalog.class
		);
		
		return Response.ok(cats).build();
	}
	
	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response catalogById(@PathParam("id") String id) {
		String filter = new Search().filter(Builder.match("id", id)).build();
		Catalog cat = cqlOps.selectOne(
			String.format("SELECT * FROM catalog WHERE lucene = '%s'", filter), 
			Catalog.class
		);
		
		Map<String, Object> mCats = new HashMap<>();
		mCats.put("id", cat.getId().toString());
		mCats.put("ddiId", cat.getDdiId());
		mCats.put("title", cat.getTitle());
		mCats.put("resources", cat.getResources());
		mCats.put("abstract", cat.getAbstract());
		mCats.put("dataKind", cat.getDataKind());
		
		try {
			String ddiFile = resourceBasedir + File.separator + "ddi" + File.separator + cat.getDdiId() + ".xml";
			DDIParser ddi = new DDIParser(new File(ddiFile));
			mCats.put("ddi", ddi);
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
		
		return Response.ok(mCats).build();
	}
	
	@SuppressWarnings("resource")
	@GET
	@Path("{id}/thumb")
	@Produces("image/png")
	public Response catalogThumbById(@PathParam("id") String id) {
		String filter = new Search().filter(Builder.match("id", id)).build();
		Catalog cat = cqlOps.selectOne(
			String.format("SELECT * FROM catalog WHERE lucene = '%s'", filter), 
			Catalog.class
		);
		
		InputStream iis = null;
		try {
			new File(resourceBasedir + File.separator + "catalog" + File.separator + "thumb").mkdirs();
			iis = new FileInputStream(resourceBasedir + File.separator + "catalog" + File.separator + 
					"thumb" + File.separator + cat.getId().toString());
		} catch (FileNotFoundException e) {
			iis = CatalogService.class.getResourceAsStream("bps.png");
		}
		
		final InputStream tis = iis;
		return Response.ok(new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException, WebApplicationException {
				IOUtils.copy(tis, output);
			}
		}).build();
	}
	
	@SuppressWarnings("resource")
	@POST
	@Path("{id}/thumb/update")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces("image/png")
	public Response importDdiFromFile(
		@PathParam("id") String id, 
		@FormDataParam("catalogthumb") final InputStream uplIS,
        @FormDataParam("catalogthumb") FormDataContentDisposition detail
	) {
		LOG.info("Received Catalog Image : " + detail.getFileName());
		
		InputStream is = null;
		try {
			IOUtils.copy(
				uplIS, 
				new FileOutputStream(resourceBasedir + File.separator + "catalog" + 
						File.separator + "thumb" + File.separator + id)
			);
			
			is = new FileInputStream(resourceBasedir + File.separator + "catalog" + 
						File.separator + "thumb" + File.separator + id);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			is = CatalogService.class.getResourceAsStream("bps.png");
		}
		
		final InputStream tis = is;
		return Response.ok(new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException, WebApplicationException {
				IOUtils.copy(tis, output);
			}
		}).build();
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
				
				@SuppressWarnings("unchecked")
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
				
				@SuppressWarnings("unchecked")
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

}
