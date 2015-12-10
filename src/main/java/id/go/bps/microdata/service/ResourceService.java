package id.go.bps.microdata.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
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
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
import com.stratio.cassandra.lucene.search.Search;
import com.stratio.cassandra.lucene.search.SearchBuilders;

import id.go.bps.microdata.filter.API;
import id.go.bps.microdata.library.DDIParser;
import id.go.bps.microdata.model.Resource;
import id.go.bps.microdata.model.ResourceFile;
import id.go.bps.microdata.model.ResourceKey;
import ru.smartflex.tools.dbf.DbfEngine;
import ru.smartflex.tools.dbf.DbfEngineException;
import ru.smartflex.tools.dbf.DbfIterator;
import ru.smartflex.tools.dbf.DbfRecord;

@API
@Path("api/resource")
public class ResourceService {	
	Logger LOG = LoggerFactory.getLogger(ResourceService.class);
	
	@Autowired
	private CassandraOperations cqlOps;
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response resources() {
		List<Object> resources = new ArrayList<>();
		
		Select s = QueryBuilder.select().from("resource").allowFiltering();
		for(Resource res : cqlOps.select(s, Resource.class)) {
			for(String resFileId : res.getFiles()) {
				Select sf = QueryBuilder.select().from("resource_file").allowFiltering();
				sf.where(QueryBuilder.eq("id", UUID.fromString(resFileId)));
				ResourceFile rFile = cqlOps.selectOne(sf, ResourceFile.class);
				
				Map<String, Object> mRes = new HashMap<>();
				mRes.put("resourceId", rFile.getId());
				mRes.put("resourceName", rFile.getFileName());
				mRes.put("resourceDescription", rFile.getFileContent());
				mRes.put("eventId", res.getPk().getId());
				mRes.put("eventName", res.getTitle());
				
				resources.add(mRes);
			}
		}
		
		return Response.ok(resources).build();
	}
	
	@GET
	@Path("{resFileId}/variables")
	@Produces(MediaType.APPLICATION_JSON)
	public Response resourceVariables(@PathParam("resFileId") String resFileId) {
		Map<String, String> map = cassandraTypeMap();
		
		Select sf = QueryBuilder.select().from("resource_file").allowFiltering();
		sf.where(QueryBuilder.eq("id", UUID.fromString(resFileId)));
		ResourceFile res = cqlOps.selectOne(sf, ResourceFile.class);
		
		if(res != null) {
			List<Object> vars = new ArrayList<>();
			for(Row row : cqlOps.query(String.format("SELECT * FROM system.schema_columns WHERE columnfamily_name='%s' AND keyspace_name='%s'", res.getTableName(), cqlOps.getSession().getLoggedKeyspace())).all()) {
				Map<String, Object> var = new HashMap<>();
				var.put("variableName", row.getString("column_name"));
				var.put("variableType", map.get(row.getString("validator")));
				vars.add(var);
			}
			
			return Response.ok(vars).build();
		}
		
		return Response.status(Status.NOT_FOUND)
				.entity(String.format("Resource %s not found", resFileId))
				.build();
	}
	
	@GET
	@Path("{resFileId}/query")
	@Produces(MediaType.APPLICATION_JSON)
	public Response resourceQuery(@PathParam("resFileId") String resFileId) {
		Select sf = QueryBuilder.select().from("resource_file").allowFiltering();
		sf.where(QueryBuilder.eq("id", UUID.fromString(resFileId)));
		ResourceFile res = cqlOps.selectOne(sf, ResourceFile.class);
		
		if(res == null) return Response.status(Status.NOT_FOUND)
				.entity(String.format("Resource %s not found", resFileId))
				.build();
		
		Search s = SearchBuilders.search().filter(SearchBuilders.bool()).build();
		
		
		Select sel = QueryBuilder.select().all().from(res.getTableName()).allowFiltering();
		sel.where(QueryBuilder.eq("lucene", "{"
				+ "filter : {"
				+ "type : \"all\""
				//+ "must : [ { type : \"match\", field : \"census_blo\", value : \"1\" } ]"
				+ "}"
			+ "}"
		));
		
		LOG.info(sel.getQueryString());
		List<Row> values = cqlOps.query(sel).all();
		
		return Response.ok(values).build();
	}
	
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
					
					//create index
					String luceneIndex = createIndex(parser, String.valueOf(file.get("id")), tableName);
					luceneIndices.add(luceneIndex);
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
	
	@POST
	@Path("{resFileId}/import/dbf")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.APPLICATION_JSON)
	public Response importResourceFromFile(@FormDataParam("dbffile") final InputStream uplIS,
            @FormDataParam("dbffile") FormDataContentDisposition detail, @PathParam("resFileId") String resFileId) {
		LOG.info("Received DBF File : " + detail.getFileName());
		String filename = new Date().getTime() + detail.getFileName();
		
		Select sf = QueryBuilder.select().from("resource_file").allowFiltering();
		sf.where(QueryBuilder.eq("id", UUID.fromString(resFileId)));
		ResourceFile res = cqlOps.selectOne(sf, ResourceFile.class);
		
		if(res == null) return Response.status(Status.NOT_FOUND)
				.entity(String.format("Resource %s not found", resFileId))
				.build();
		
		Map<String, String> map = cassandraTypeMap();
		List<Row> cols = cqlOps.query(String.format("SELECT * FROM system.schema_columns WHERE columnfamily_name='%s' AND keyspace_name='%s'", res.getTableName(), cqlOps.getSession().getLoggedKeyspace())).all();
		
		try {
			new File("dbf/" + filename).getParentFile().mkdirs();
			IOUtils.copyLarge(uplIS, new FileOutputStream("dbf/" + filename));
			
			List<Insert> lstIns = new ArrayList<>();
			DbfIterator iter = DbfEngine.getReader("dbf/" + filename, null);
			while (iter.hasMoreRecords()) {
				DbfRecord rec = iter.nextRecord();
				
				Insert ins = QueryBuilder.insertInto(res.getTableName());
				ins.value("id", UUIDs.timeBased().toString());
				for(Row col : cols) {
					String colName = col.getString("column_name");
					String type = map.get(col.getString("validator"));
					
					try {
						if(type.equalsIgnoreCase("int")) ins.value(colName, rec.getInt(colName));
						else if(type.equalsIgnoreCase("text")) ins.value(colName, rec.getString(colName));
						else if(type.equalsIgnoreCase("decimal")) ins.value(colName, rec.getFloat(colName));
					} catch(DbfEngineException dbfE) {}
					
					lstIns.add(ins);
				}
			}
			
			for(Insert ins : lstIns) cqlOps.execute(ins);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error in uploading file(s)").build();
		} catch (DataAccessException e) {
			LOG.error(e.getMessage());
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error in database access").build();
		}
		
		return Response.ok().build();
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
	
	private Map<String, String> cassandraTypeMap() {
		Map<String, String> map = new HashMap<>();
		map.put("org.apache.cassandra.db.marshal.AsciiType",         "ascii");
		map.put("org.apache.cassandra.db.marshal.LongType",          "bigint");
		map.put("org.apache.cassandra.db.marshal.BytesType",         "blob");
		map.put("org.apache.cassandra.db.marshal.BooleanType",       "boolean");
		map.put("org.apache.cassandra.db.marshal.CounterColumnType", "counter");
		map.put("org.apache.cassandra.db.marshal.DecimalType",       "decimal");
		map.put("org.apache.cassandra.db.marshal.DoubleType",        "double");
		map.put("org.apache.cassandra.db.marshal.FloatType",         "float");
		map.put("org.apache.cassandra.db.marshal.InetAddressType",   "inet");
		map.put("org.apache.cassandra.db.marshal.Int32Type",         "int");
		map.put("org.apache.cassandra.db.marshal.UTF8Type",          "text");
		map.put("org.apache.cassandra.db.marshal.TimestampType",     "timestamp");
		map.put("org.apache.cassandra.db.marshal.DateType",          "timestamp");
		map.put("org.apache.cassandra.db.marshal.UUIDType",          "uuid");
		map.put("org.apache.cassandra.db.marshal.IntegerType",       "varint");
		map.put("org.apache.cassandra.db.marshal.TimeUUIDType",      "timeuuid");
		
		return map;
	}

}
