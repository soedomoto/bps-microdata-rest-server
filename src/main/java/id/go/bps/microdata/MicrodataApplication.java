package id.go.bps.microdata;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;

import id.go.bps.microdata.library.ClasspathUtil;

@SpringBootApplication
public class MicrodataApplication extends SpringBootServletInitializer {
	
	@Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(MicrodataApplication.class);
    }
	
    public static void main(String[] args) {
    	ClasspathUtil cpU;
		try {
			File jarFile = new File(MicrodataApplication.class.getProtectionDomain().getCodeSource().getLocation().toURI());
	    	File cd = new File(jarFile.getParentFile().getAbsolutePath() + File.separator + "conf");
			
			cpU = new ClasspathUtil();
			cpU.addDirectoryClasspath(cd, false);
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | MalformedURLException | URISyntaxException e1) {
			e1.printStackTrace();
		}
    	
    	SpringApplication.run(MicrodataApplication.class, args);
    }
    
}
