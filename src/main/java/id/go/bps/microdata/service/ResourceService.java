package id.go.bps.microdata.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stratio.cassandra.lucene.builder.search.Search;
import com.stratio.cassandra.lucene.builder.search.condition.Condition;

import id.go.bps.microdata.filter.API;
import id.go.bps.microdata.model.Catalog;
import id.go.bps.microdata.model.Resource;
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
	@Autowired 
	@Qualifier("resourceBasedir")
	private String resourceBasedir;
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response resources() {
		List<Object> resources = new ArrayList<>();
		
		Select s = QueryBuilder.select().from("resource").allowFiltering();
		for(Catalog res : cqlOps.select(s, Catalog.class)) {
			for(String resFileId : res.getFiles()) {
				Select sf = QueryBuilder.select().from("resource").allowFiltering();
				sf.where(QueryBuilder.eq("id", UUID.fromString(resFileId)));
				Resource rFile = cqlOps.selectOne(sf, Resource.class);
				
				Map<String, Object> mRes = new HashMap<>();
				mRes.put("resourceId", rFile.getId());
				mRes.put("resourceName", rFile.getFileName());
				mRes.put("resourceDescription", rFile.getFileContent());
				mRes.put("eventId", res.getId());
				mRes.put("eventName", res.getTitle());
				
				resources.add(mRes);
			}
		}
		
		return Response.ok(resources).build();
	}
	
	@GET
	@Path("{resFileId}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response resourceDetail(@PathParam("resFileId") String resFileId) {
		String cql = String.format(
			"SELECT * FROM resource WHERE lucene = '{ filter : { " + 
				"type  : \"match\", " + 
				"field : \"%s\", " + 
				"value : \"%s\" " + 
			"} }'", 
			"id", 
			resFileId
		);		
		
		try {
			return Response.ok(cqlOps.selectOne(cql, Resource.class)).build();
		} catch(Exception e) {
			LOG.error(e.getMessage());
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Something wrong happened").build();
		}
	}
	
	@GET
	@Path("{resFileId}/variables")
	@Produces(MediaType.APPLICATION_JSON)
	public Response resourceVariables(@PathParam("resFileId") String resFileId) {
		Map<String, String> map = cassandraTypeMap();
		
		String cql = String.format(
			"SELECT * FROM resource WHERE lucene = '{ filter : { " + 
				"type  : \"match\", " + 
				"field : \"%s\", " + 
				"value : \"%s\" " + 
			"} }'", 
			"id", 
			resFileId
		);	
		
		Resource res = cqlOps.selectOne(cql, Resource.class);
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
	public Response resourceQuery(@PathParam("resFileId") String resFileId, 
		@QueryParam("variables") String variables, 
		@QueryParam("filter") String filter
	) {
		String sel = "*";
		if(variables != null) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				@SuppressWarnings("unchecked")
				List<String> lstVars = mapper.readValue(variables, ArrayList.class);
				if(lstVars.size() > 0) sel = StringUtil.join(lstVars, ",");
			} catch (IOException e) {
				LOG.error(e.getMessage());
			}
		}
		
		Search s = new Search();
		
		String fil = s.filter(s.all()).build();
		if(filter != null) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				@SuppressWarnings("unchecked")
				Map<String, Map<String, String>> lstFilters = mapper.readValue(filter, HashMap.class);
				
				if(lstFilters.keySet().size() > 0) {
					List<Condition> conds = new ArrayList<>();
					for(Object op : lstFilters.keySet().toArray()) {
						Map<String, String> keyVal = lstFilters.get(op);
						
						if(String.valueOf(op).equalsIgnoreCase("=")) {
							for(Object key : keyVal.keySet().toArray()) {
								conds.add(s.match(String.valueOf(key), keyVal.get(key)));
							}
						}
					}
					
					fil = s.filter(s.bool().must(conds.toArray(new Condition[conds.size()]))).build();
				}
			} catch (IOException e) {
				LOG.error(e.getMessage());
			}
		}
		
		String cql = String.format(
			"SELECT * FROM resource WHERE lucene = '{ filter : { " + 
				"type  : \"match\", " + 
				"field : \"%s\", " + 
				"value : \"%s\" " + 
			"} }'", 
			"id", resFileId
		);	
		
		Resource res = cqlOps.selectOne(cql, Resource.class);		
		if(res == null) return Response.status(Status.NOT_FOUND)
				.entity(String.format("Resource %s not found", resFileId))
				.build();
		
		String cqlRes = String.format(
			"SELECT %s FROM %s WHERE lucene = '%s'", 
			sel, res.getTableName(), fil
		);
		//LOG.info(cqlRes);
		final ResultSet rs = cqlOps.query(cqlRes);
		
		Map<String, Class<?>> type = new HashMap<>();
		for(Definition colDef : rs.getColumnDefinitions().asList()) {
			type.put(colDef.getName(), colDef.getType().asJavaClass());
		}
		
		List<Map<String, Object>> table = new ArrayList<>();
		for(Row row : rs.all()) {
			Map<String, Object> lRow = new HashMap<>();
			for(int t=0; t<type.keySet().size(); t++) {
				String col = String.valueOf(type.keySet().toArray()[t]);
				Class<?> typ = type.get(col);
				
				if(typ == String.class) lRow.put(col, row.getString(col));
				else if(typ == Integer.class) lRow.put(col, row.getInt(col));
				else if(typ == Float.class) lRow.put(col, row.getFloat(col));
				else if(typ == Double.class) lRow.put(col, row.getDouble(col));
			}
			
			table.add(lRow);
		}
		
		Map<String, Object> result = new HashMap<>();
		result.put("columns", type.keySet());
		result.put("table", table);
		
		return Response.ok(result).build();
	}
	
	@POST
	@Path("{resFileId}/import/dbf/zip")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.APPLICATION_JSON)
	public Response importResourceFromZipFile(@FormDataParam("dbffile") final InputStream uplIS,
            @FormDataParam("dbffile") FormDataContentDisposition detail, @PathParam("resFileId") String resFileId) {		
		LOG.info("Received Zipped DBF File : " + detail.getFileName());
		String dirOut = resourceBasedir + File.separator + "dbf";
		
		ZipInputStream zis = new ZipInputStream(uplIS);
		try {
			new File(dirOut).mkdirs();
			
			byte[] buffer = new byte[1024];
			ZipEntry ze = zis.getNextEntry();
			while(ze != null) {
				String fileName = dirOut + File.separator + new Date().getTime() + ze.getName();
				FileOutputStream fos = new FileOutputStream(fileName);
				
				int len;
	            while ((len = zis.read(buffer)) > 0) {
	            	fos.write(buffer, 0, len);
	            }
	            
	            fos.close();
	            
	            return processDbf(new FileInputStream(fileName), resFileId);
			}
			
			zis.closeEntry();
	    	zis.close();
		} catch (IOException e) {
			LOG.error(e.getMessage());
			Map<String, Object> resp = new HashMap<>();
			resp.put("success", false);
			resp.put("error", "Error in uploading file");
			return Response.ok(resp).build();
		}
		
		return Response.ok().build();
	}
	
	private Response processDbf(InputStream uplIs, String resFileId) {
		Map<String, Object> resp = new HashMap<>();
		boolean success = true;
		
		try {
			String cql = String.format(
				"SELECT * FROM resource WHERE lucene = '{ filter : { " + 
					"type  : \"match\", " + 
					"field : \"%s\", " + 
					"value : \"%s\" " + 
				"} }'", 
				"id", 
				UUID.fromString(resFileId)
			);	
			Resource res = cqlOps.selectOne(cql, Resource.class);
			
			if(res == null) {
				success = false;
				resp.put("error", String.format("Resource %s not found", resFileId));
			}
			
			Map<String, String> map = cassandraTypeMap();
			List<Row> cols = cqlOps.query(String.format("SELECT * FROM system.schema_columns WHERE columnfamily_name='%s' AND keyspace_name='%s'", res.getTableName(), cqlOps.getSession().getLoggedKeyspace())).all();
			
			List<Insert> lstIns = new ArrayList<>();
			DbfIterator iter = DbfEngine.getReader(uplIs, null);
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
			resp.put("result", "Succesfully processing dbf file(s)");
		} catch(DataAccessException e) {
			LOG.error(e.getMessage());
			success = false;
			resp.put("error", "Error in database access");
		}
		
		resp.put("success", success);
		return Response.ok(resp).build();
	}
	
	@POST
	@Path("{resFileId}/import/dbf")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.APPLICATION_JSON)
	public Response importResourceFromFile(@FormDataParam("dbffile") final InputStream uplIS,
            @FormDataParam("dbffile") FormDataContentDisposition detail, @PathParam("resFileId") String resFileId) {		
		LOG.info("Received DBF File : " + detail.getFileName());
		String fileOut = resourceBasedir + File.separator + "dbf" + File.separator + new Date().getTime() + detail.getFileName();
		
		try {
			new File(fileOut).getParentFile().mkdirs();
			IOUtils.copyLarge(uplIS, new FileOutputStream(fileOut));
			
			return processDbf(new FileInputStream(fileOut), resFileId);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			Map<String, Object> resp = new HashMap<>();
			resp.put("success", false);
			resp.put("error", "Error in uploading file");
			return Response.ok(resp).build();
		}
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
