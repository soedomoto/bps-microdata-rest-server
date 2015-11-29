package id.go.bps.microdata.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

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
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;

import id.go.bps.microdata.filter.API;
import id.go.bps.microdata.library.DDIParser;
import id.go.bps.microdata.model.Resource;
import id.go.bps.microdata.model.ResourceFile;
import id.go.bps.microdata.model.ResourceKey;

@API
@Path("api/resource")
public class ResourceService {
	
Logger LOG = LoggerFactory.getLogger(ResourceService.class);
	
	@Autowired
	private CassandraOperations cqlOps;
	
	@POST
	@Path("import/ddi")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.APPLICATION_XML)
	public Response importDdiFromFile(@FormDataParam("ddifile") final InputStream uplIS,
            @FormDataParam("ddifile") FormDataContentDisposition detail) {
		LOG.info(detail.getFileName());
		
		try {
			IOUtils.copyLarge(uplIS, new FileOutputStream("ddi/" + detail.getFileName()));			
			DDIParser parser = new DDIParser(new File("ddi/" + detail.getFileName()));
			
			List<CreateTableSpecification> cqlTables = new ArrayList<>();
			
			Select s = QueryBuilder.select().from("resource").allowFiltering();
			s.where(QueryBuilder.eq("ddi_id", parser.getIDNo()));
			Resource res = cqlOps.selectOne(s, Resource.class);
			if(res == null) {
				List<String> idFiles = new ArrayList<>();
				List<ResourceFile> rFiles = new ArrayList<>();
				for(Map<String, Object> file : parser.getFileDescription()) {
					String tableName = "t" + parser.getIDNo().concat(String.valueOf(file.get("id")))
							.replace("-", "").toLowerCase();
					
					ResourceFile rFile = new ResourceFile(
							UUIDs.timeBased(), 
							String.valueOf(file.get("id")), 
							String.valueOf(file.get("fileName")).replace(".NSDstat", ""), 
							String.valueOf(file.get("fileCont")), 
							tableName
						);
					rFiles.add(rFile);
					idFiles.add(rFile.getId().toString());
					
					//create table
					CreateTableSpecification cqlTable = createTable(parser, String.valueOf(file.get("id")), tableName);
					cqlTables.add(cqlTable);
				}
				cqlOps.insert(rFiles);
				
				res = new Resource();
				res.setPk(new ResourceKey(UUIDs.timeBased(), parser.getIDNo()));
				res.setTitle(parser.getTitle());
				res.setFile(idFiles);
				cqlOps.insert(res);
			} else {
				List<ResourceFile> rFiles = new ArrayList<>();
				List<Map<String, Object>> files = parser.getFileDescription();
				for(String idFile : res.getFiles()) {
					Select sf = QueryBuilder.select().from("resource_file").allowFiltering();
					sf.where(QueryBuilder.eq("id", UUID.fromString(idFile)));
					ResourceFile rFile = cqlOps.selectOne(sf, ResourceFile.class);
					
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
				
				res.setTitle(parser.getTitle());
				cqlOps.update(res);
			}
			
			for(CreateTableSpecification cql : cqlTables) cqlOps.execute(cql);
			
			return Response.ok().build();
		} catch (IOException e) {
			LOG.error(e.getMessage());
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error in uploading file(s)").build();
		} catch (DataAccessException e) {
			LOG.error(e.getMessage());
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error in database access").build();
		}
	}
	
	private CreateTableSpecification createTable(DDIParser parser, String fileId, String tableName) {
		CreateTableSpecification creTabSpec = CreateTableSpecification.createTable(tableName).partitionKeyColumn("id", DataType.timeuuid());
		
		List<String> columns = new ArrayList<>();
		columns.add("id UUID PRIMARY KEY");
		for(Map<String, Object> var : parser.getVariables()) {
			String file = String.valueOf(var.get("file"));
			String varName = String.valueOf(var.get("name")).toLowerCase();
			
			if(fileId.equalsIgnoreCase(file)) {
				DataType type = DataType.text();
				
				Map<String, Object> valueRange = (Map<String, Object>) var.get("valueRange");
				String varUnit = String.valueOf(valueRange.get("unit"));
				if(varUnit.equalsIgnoreCase("REAL"))
					type = DataType.cint();
				else if(var.get("decimal") != null && !String.valueOf(var.get("decimal")).isEmpty() && 
						Integer.valueOf(String.valueOf(var.get("decimal"))) > 0) 
					type = DataType.decimal();
				
				creTabSpec.column(varName, type);
			}
		}
		
		return creTabSpec;
	}

}
