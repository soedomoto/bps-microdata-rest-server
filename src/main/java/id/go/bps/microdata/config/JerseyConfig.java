package id.go.bps.microdata.config;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.context.annotation.Configuration;

import id.go.bps.microdata.filter.ResourceFilterBindingFeature;

@Configuration
public class JerseyConfig extends ResourceConfig {
    public JerseyConfig() {
    	register(ResourceFilterBindingFeature.class);
    	register(MultiPartFeature.class);
        packages("id.go.bps.microdata");
    }
}
