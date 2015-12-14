package id.go.bps.microdata.filter;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.datastax.driver.core.exceptions.InvalidQueryException;

import id.go.bps.microdata.library.CassandraUtil;
import id.go.bps.microdata.model.Catalog;
import id.go.bps.microdata.model.Resource;

public class CassandraIndexFilter implements ContainerRequestFilter {
	private Logger LOG = LoggerFactory.getLogger(CassandraIndexFilter.class);
	
	@Autowired
	private CassandraOperations cqlOps;

	@Override
	public void filter(ContainerRequestContext requestContext) throws IOException {
		try {
			String luceneIndexCql = CassandraUtil.luceneIndexCql("catalog", Catalog.class, 1);
			//LOG.info(luceneIndexCql);
			cqlOps.execute(luceneIndexCql);
		} catch (InvalidQueryException e) {
			if(!e.getMessage().equalsIgnoreCase("Index already exists")) 
				LOG.info(e.getMessage());
		}
		
		try {
			String luceneIndexCql = CassandraUtil.luceneIndexCql("resource", Resource.class, 1);
			//LOG.info(luceneIndexCql);
			cqlOps.execute(luceneIndexCql);
		} catch (InvalidQueryException e) {
			if(!e.getMessage().equalsIgnoreCase("Index already exists"))
				LOG.info(e.getMessage());
		}
	}

}
