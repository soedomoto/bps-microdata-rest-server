package id.go.bps.microdata.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;

import javax.crypto.spec.SecretKeySpec;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.PropertySource;

import io.jsonwebtoken.impl.crypto.MacProvider;

@Configuration
@DependsOn({"currentDirectory"})
@PropertySource({ "classpath:application.properties" })
public class TokenConfig {
	@Autowired
	String currentDirectory;
	
	@Bean(name = "generatedRsaKey")
	public Key generatedRsaKey() {
		Key key = MacProvider.generateKey();
		return key;
	}
	
	@Bean(name = "rsaKey")
	public Key rsaKey() throws IOException {
		byte[] encodedKey = Files.readAllBytes(Paths.get(currentDirectory + File.separator + "cert/jwt.key"));
		Key originalKey = new SecretKeySpec(encodedKey, 0, encodedKey.length, "HS512");
		return originalKey;
	}
	
}
