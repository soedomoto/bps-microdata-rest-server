package id.go.bps.microdata.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.config.java.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

@Configuration
@PropertySource({ "classpath:application.properties" })
@EnableCassandraRepositories(basePackages = { "id.go.bps.microdata" })
public class CassandraConfig extends AbstractCassandraConfiguration {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(CassandraConfig.class);

	@Autowired
	private Environment env;
	
	@Override
	protected String getContactPoints() {
		return env.getProperty("cassandra.contactpoints");
	}
	
	@Override
	protected int getPort() {
		return Integer.parseInt(env.getProperty("cassandra.port"));
	}
	
	@Override
	protected String getKeyspaceName() {
		return env.getProperty("cassandra.keyspace");
	}
	
	@Override
    public SchemaAction getSchemaAction() {
        return SchemaAction.RECREATE_DROP_UNUSED;
		//return SchemaAction.NONE;
    }
	
	@Override
	public String[] getEntityBasePackages() {
	    return new String[] {"id.go.bps.microdata"};
	}
	
}
