package id.go.bps.microdata.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
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

import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;

import id.go.bps.microdata.filter.API;
import id.go.bps.microdata.library.CassandraUtil;
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
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response resources() {
		List<Object> resources = new ArrayList<>();
		
		Select s = QueryBuilder.select().from("resource").allowFiltering();
		for(Catalog res : cqlOps.select(s, Catalog.class)) {
			for(String resFileId : res.getFiles()) {
				Select sf = QueryBuilder.select().from("resource_file").allowFiltering();
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
	public Response resourceQuery(@PathParam("resFileId") String resFileId) {		
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
		if(res == null) return Response.status(Status.NOT_FOUND)
				.entity(String.format("Resource %s not found", resFileId))
				.build();
		
		String cqlRes = String.format(
			"SELECT * FROM %s WHERE lucene = '{ filter : { " + 
				"type  : \"all\"" + 
			"} }'", 
			res.getTableName()
		);
		
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
	@Path("{resFileId}/import/dbf")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.APPLICATION_JSON)
	public Response importResourceFromFile(@FormDataParam("dbffile") final InputStream uplIS,
            @FormDataParam("dbffile") FormDataContentDisposition detail, @PathParam("resFileId") String resFileId) {
		try {
			String luceneIndexCql = CassandraUtil.luceneIndexCql("resource", Resource.class, 1);
			cqlOps.execute(luceneIndexCql);
		} catch (InvalidQueryException e) {
			LOG.info(e.getMessage());
		}
		
		LOG.info("Received DBF File : " + detail.getFileName());
		String filename = new Date().getTime() + detail.getFileName();
		
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
