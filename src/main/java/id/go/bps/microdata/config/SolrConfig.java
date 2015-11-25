package id.go.bps.microdata.config;

import java.net.MalformedURLException;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.solr.repository.config.EnableSolrRepositories;

@Configuration
@PropertySource({ "classpath:application.properties" })
@EnableSolrRepositories(basePackages = {"id.go.bps.microdata"}, multicoreSupport = true)
public class SolrConfig {
	
	@Value("${solr.host}")
    private String solrHost;

    @Bean
    public SolrServer solrServer() throws MalformedURLException, IllegalStateException {
        return new HttpSolrServer(solrHost);
    }
	
}
