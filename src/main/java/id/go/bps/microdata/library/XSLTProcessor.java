package id.go.bps.microdata.library;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

public class XSLTProcessor {

	public static void main(String[] args) throws TransformerException {
		TransformerFactory tFactory = TransformerFactory.newInstance();
		Transformer transformer = tFactory.newTransformer(new StreamSource("xslt/ddi_datafiles_list.xslt"));
		
		transformer.transform(
			new StreamSource("00-SP-2010-M1.xml"), 
			new StreamResult(System.out)
		);
	}

}
