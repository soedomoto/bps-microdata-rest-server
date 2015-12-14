package id.go.bps.microdata.library;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.jsoup.helper.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;

public class CassandraUtil {
	private static Logger LOG = LoggerFactory.getLogger(CassandraUtil.class);
	
	public static String luceneIndexCql(String tableName, Class<?> tableClazz, int refresh) {
		List<String> sFields = new ArrayList<>();
		for(Field field : tableClazz.getDeclaredFields()) {
			if(field.isAnnotationPresent(Column.class) || field.isAnnotationPresent(PrimaryKeyColumn.class) || 
					field.isAnnotationPresent(PrimaryKey.class)) {
				String type = "text";
				if(field.getType().equals(UUID.class)) type = "uuid";
				else if(field.getType().equals(String.class)) type = "text";
				else if(field.getType().equals(Long.class)) type = "long";
				else if(field.getType().equals(Integer.class)) type = "integer";
				else if(field.getType().equals(Float.class)) type = "float";
				else if(field.getType().equals(Double.class)) type = "double";
				else if(field.getType().equals(Date.class)) type = "date";
				else if(field.getType().equals(Boolean.class)) type = "boolean";
				
				String name = field.getName();
				if(field.isAnnotationPresent(Column.class)) {
					String nameA = field.getAnnotation(Column.class).value();
					if(!nameA.isEmpty()) name = nameA;
				} else if(field.isAnnotationPresent(PrimaryKeyColumn.class)) {
					String nameA = field.getAnnotation(PrimaryKeyColumn.class).name();
					if(!nameA.isEmpty()) name = nameA;
				} else if(field.isAnnotationPresent(PrimaryKey.class)) {
					String nameA = field.getAnnotation(PrimaryKey.class).value();
					if(!nameA.isEmpty()) name = nameA;
				}
				
				String sField = String.format("\"%s\" : { type : \"%s\" }", name, type);
				sFields.add(sField);
			}
		}
		
		List<String> options = new ArrayList<>();
		options.add(String.format("'refresh_seconds' : '%d'", refresh));
		options.add(String.format("'schema' : '{ fields : { %s } }'", StringUtil.join(sFields, " , ")));
		
		return String.format(
				"CREATE CUSTOM INDEX %s_index ON %s (lucene) " + 
				"USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = " + 
				"{ %s }", tableName, tableName, StringUtil.join(options, " , "));
	}
	
}
