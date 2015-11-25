package id.go.bps.microdata;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import id.go.bps.microdata.library.ClasspathUtil;

@SpringBootApplication
public class MicrodataApplication {
    public static void main(String[] args) {
    	File cd = new File(System.getProperty("user.dir") + File.separator + "conf");
    	
    	ClasspathUtil cpU;
		try {
			cpU = new ClasspathUtil();
			cpU.addDirectoryClasspath(cd, false);
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | MalformedURLException e1) {
			e1.printStackTrace();
		}
    	
    	SpringApplication.run(MicrodataApplication.class, args);
    }
}
