package id.go.bps.microdata.config;

import java.io.File;
import java.net.URISyntaxException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import id.go.bps.microdata.MicrodataApplication;

@Configuration
@PropertySource({ "classpath:application.properties" })
public class ResourceConfig {
	@Value("${resource.basedir}")
	private String basedir;
	
	@Bean(name = "resourceBasedir")
	public String resourceBasedir() {
		return basedir;
	}
	
	@Bean(name = "currentDirectory")
	public String currentDirectory() throws URISyntaxException {
		File jarFile = new File(MicrodataApplication.class.getProtectionDomain().getCodeSource().getLocation().toURI());
		return jarFile.getParentFile().getAbsolutePath();
	}

}
