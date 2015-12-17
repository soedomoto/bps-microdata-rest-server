package id.go.bps.microdata.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource({ "classpath:application.properties" })
public class ResourceConfig {
	@Value("${resource.basedir}")
	private String basedir;
	
	@Bean(name = "resourceBasedir")
	public String resourceBasedir() {
		return basedir;
	}

}
