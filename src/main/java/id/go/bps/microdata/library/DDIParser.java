package id.go.bps.microdata.library;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class DDIParser {
	private Document doc;
	
	public DDIParser(File input) throws IOException {
		doc = Jsoup.parse(input, "UTF-8");
	}
	
	/**
	 * Under tag : stdyDscr > citation > titlStmt > titl
	 */
	public String getTitle() {
		return doc.select("stdyDscr citation titlStmt titl").text();
	}
	
	/**
	 * Under tag : stdyDscr > citation > titlStmt > altTitl
	 */
	public String getAltTitle() {
		return doc.select("stdyDscr citation titlStmt altTitl").text();
	}
	
	/**
	 * Under tag : stdyDscr > citation > titlStmt > IDNo
	 */
	public String getIDNo() {
		return doc.select("stdyDscr citation titlStmt IDNo").text();
	}
	
	/**
	 * Under tag : stdyDscr > citation > titlStmt > AuthEnty
	 */
	public Map<String, Object> getAuthEnty() {
		Map<String, Object> auths = new HashMap<>();
		auths.put("AuthEnty", doc.select("stdyDscr citation rspStmt AuthEnty").text());
		auths.put("affiliation", doc.select("stdyDscr citation rspStmt AuthEnty").attr("affiliation"));
		return auths;
	}
	
	/**
	 * Under tag : stdyDscr > citation > prodStmt > producer
	 */
	public Map<String, Object> getProducer() {
		Map<String, Object> producers = new HashMap<>();
		producers.put("producer", doc.select("stdyDscr citation prodStmt producer").text());
		producers.put("affiliation", doc.select("stdyDscr citation prodStmt producer").attr("affiliation"));
		return producers;
	}
	
	/**
	 * Under tag : stdyDscr > citation > prodStmt > copyright
	 */
	public String getCopyright() {
		return doc.select("stdyDscr citation prodStmt copyright").text();
	}
	
	/**
	 * Under tag : stdyDscr > citation > prodStmt > software
	 */
	public Map<String, Object> getSoftware() {
		Map<String, Object> software = new HashMap<>();
		software.put("software", doc.select("stdyDscr citation prodStmt software").text());
		software.put("version", doc.select("stdyDscr citation prodStmt software").attr("version"));
		software.put("date", doc.select("stdyDscr citation prodStmt software").attr("date"));
		return software;
	}
	
	/**
	 * Under tag : stdyDscr > citation > prodStmt > fundAg
	 */
	public String getFunder() {
		return doc.select("stdyDscr citation prodStmt fundAg").text();
	}
	
	/**
	 * Under tag : stdyDscr > citation > distStmt > contact
	 */
	public List<String> getContact() {
		List<String> contacts = new ArrayList<>();
		Iterator<Element> it = doc.select("stdyDscr citation distStmt contact").iterator();
		while(it.hasNext()) {
			contacts.add(it.next().text());
		}
		
		return contacts;
	}
	
	/**
	 * Under tag : stdyDscr > citation > serStmt
	 */
	public Map<String, Object> getSer() {
		Map<String, Object> sers = new HashMap<>();
		sers.put("name", doc.select("stdyDscr citation serStmt serName").text());
		sers.put("info", doc.select("stdyDscr citation serStmt serInfo").text());
		return sers;
	}
	
	/**
	 * Under tag : stdyDscr > citation > verStmt > version
	 */
	public String getVersion() {
		return doc.select("stdyDscr citation verStmt version").text();
	}
	
	/**
	 * Under tag : stdyDscr > citation > notes
	 */
	public String getCitationNotes() {
		return doc.select("stdyDscr citation notes").text();
	}
	
	/**
	 * Under tag : stdyDscr > stdyInfo > subject > keyword
	 */
	public List<String> getKeyword() {
		List<String> keywords = new ArrayList<>();
		Iterator<Element> it = doc.select("stdyDscr stdyInfo subject keyword").iterator();
		while(it.hasNext()) {
			keywords.add(it.next().text());
		}
		
		return keywords;
	}
	
	/**
	 * Under tag : stdyDscr > stdyInfo > subject > topcClas
	 */
	public List<Map<String, Object>> getTopcClas() {
		List<Map<String, Object>> keywords = new ArrayList<>();
		Iterator<Element> it = doc.select("stdyDscr stdyInfo subject topcClas").iterator();
		while(it.hasNext()) {
			Element el = it.next();
			
			Map<String, Object> props = new HashMap<>();
			props.put("topcclas", el.text());
			props.put("vocab", el.attr("vocab"));
			keywords.add(props);
		}
		
		return keywords;
	}
	
	/**
	 * Under tag : stdyDscr > stdyInfo > abstract
	 */
	public String getAbstract() {
		return doc.select("stdyDscr stdyInfo abstract").text();
	}
	
	/**
	 * Under tag : stdyDscr > stdyInfo > sumDscr > collDate
	 */
	public List<Map<String, Object>> getCollectionDate() {
		List<Map<String, Object>> keywords = new ArrayList<>();
		Iterator<Element> it = doc.select("stdyDscr stdyInfo sumDscr collDate").iterator();
		while(it.hasNext()) {
			Element el = it.next();
			
			Map<String, Object> colldates = new HashMap<>();
			colldates.put("date", el.attr("date"));
			colldates.put("event", el.attr("event"));
			colldates.put("cycle", el.attr("cycle"));
			keywords.add(colldates);
		}
		
		return keywords;
	}
	
	/**
	 * Under tag : stdyDscr > stdyInfo > sumDscr > nation
	 */
	public Map<String, Object> getNation() {
		Map<String, Object> nations = new HashMap<>();
		nations.put("nation", doc.select("stdyDscr stdyInfo sumDscr nation").text());
		nations.put("abbr", doc.select("stdyDscr stdyInfo sumDscr nation").attr("abbr"));
		return nations;
	}
	
	/**
	 * Under tag : stdyDscr > stdyInfo > sumDscr > geogCover
	 */
	public String getGeogCover() {
		return doc.select("stdyDscr stdyInfo sumDscr geogCover").text();
	}
	
	/**
	 * Under tag : stdyDscr > stdyInfo > sumDscr > geogUnit
	 */
	public String getGeogUnit() {
		return doc.select("stdyDscr stdyInfo sumDscr geogUnit").text();
	}
	
	/**
	 * Under tag : stdyDscr > stdyInfo > sumDscr > anlyUnit
	 */
	public String getAnlyUnit() {
		return doc.select("stdyDscr stdyInfo sumDscr anlyUnit").text();
	}
	
	/**
	 * Under tag : stdyDscr > stdyInfo > sumDscr > universe
	 */
	public String getUniverse() {
		return doc.select("stdyDscr stdyInfo sumDscr universe").text();
	}
	
	/**
	 * Under tag : stdyDscr > stdyInfo > sumDscr > dataKind
	 */
	public String getDataKind() {
		return doc.select("stdyDscr stdyInfo sumDscr dataKind").text();
	}
	
	/**
	 * Under tag : stdyDscr > stdyInfo > notes
	 */
	public String getStudyNotes() {
		return doc.select("stdyDscr stdyInfo notes").text();
	}
	
	/**
	 * Under tag : stdyDscr > method > dataColl > timeMeth
	 */
	public String getTimeMeth() {
		return doc.select("stdyDscr method dataColl timeMeth").text();
	}
	
	/**
	 * Under tag : stdyDscr > method > dataColl > dataCollector
	 */
	public List<Map<String, Object>> getDataCollector() {
		List<Map<String, Object>> keywords = new ArrayList<>();
		Iterator<Element> it = doc.select("stdyDscr method dataColl dataCollector").iterator();
		while(it.hasNext()) {
			Element el = it.next();
			
			Map<String, Object> colldates = new HashMap<>();
			colldates.put("collector", el.text());
			colldates.put("affiliation", el.attr("affiliation"));
			keywords.add(colldates);
		}
		
		return keywords;
	}
	
	/**
	 * Under tag : stdyDscr > method > dataColl > frequenc
	 */
	public String getFrequency() {
		return doc.select("stdyDscr method dataColl frequenc").text();
	}
	
	/**
	 * Under tag : stdyDscr > method > dataColl > sampProc
	 */
	public String getSampProc() {
		return doc.select("stdyDscr method dataColl sampProc").text();
	}
	
	/**
	 * Under tag : stdyDscr > method > dataColl > collMode
	 */
	public String getCollMode() {
		return doc.select("stdyDscr method dataColl collMode").text();
	}
	
	/**
	 * Under tag : stdyDscr > method > dataColl > sources > dataSrc
	 */
	public String getDataSrc() {
		return doc.select("stdyDscr method dataColl sources dataSrc").text();
	}
	
	/**
	 * Under tag : stdyDscr > method > dataColl > actMin
	 */
	public String getActMin() {
		return doc.select("stdyDscr method dataColl actMin").text();
	}
	
	/**
	 * Under tag : stdyDscr > method > dataColl > cleanOps
	 */
	public String getCleanOps() {
		return doc.select("stdyDscr method dataColl cleanOps").text();
	}
	
	/**
	 * Under tag : stdyDscr > method > notes
	 */
	public String getMethodNotes() {
		return doc.select("stdyDscr method notes").text();
	}
	
	/**
	 * Under tag : stdyDscr > method > anlyInfo > respRate
	 */
	public String getRespRate() {
		return doc.select("stdyDscr method anlyInfo respRate").text();
	}
	
	/**
	 * Under tag : stdyDscr > method > anlyInfo > estSmpErr
	 */
	public String getEstSmpErr() {
		return doc.select("stdyDscr method anlyInfo estSmpErr").text();
	}
	
	/**
	 * Under tag : stdyDscr > method > anlyInfo > dataAppr
	 */
	public String getDataAppr() {
		return doc.select("stdyDscr method anlyInfo dataAppr").text();
	}
	
	/**
	 * Under tag : stdyDscr > dataAccs > setAvail > accsPlac
	 */
	public List<String> getAccsPlac() {
		List<String> keywords = new ArrayList<>();
		Iterator<Element> it = doc.select("stdyDscr dataAccs setAvail accsPlac").iterator();
		while(it.hasNext()) {
			keywords.add(it.next().text());
		}
		
		return keywords;
	}
	
	/**
	 * Under tag : stdyDscr > dataAccs > setAvail > avlStatus
	 */
	public String getAvlStatus() {
		return doc.select("stdyDscr dataAccs setAvail avlStatus").text();
	}
	
	/**
	 * Under tag : stdyDscr > dataAccs > setAvail > collSize
	 */
	public String getCollSize() {
		return doc.select("stdyDscr dataAccs setAvail collSize").text();
	}
	
	/**
	 * Under tag : stdyDscr > dataAccs > useStmt > confDec
	 */
	public Map<String, Object> getConfDec() {
		Map<String, Object> nations = new HashMap<>();
		nations.put("confDec", doc.select("stdyDscr dataAccs useStmt confDec").text());
		nations.put("required", doc.select("stdyDscr dataAccs useStmt confDec").attr("required"));
		return nations;
	}
	
	/**
	 * Under tag : stdyDscr > dataAccs > useStmt > contact
	 */
	public Map<String, Object> getDataContact() {
		Map<String, Object> nations = new HashMap<>();
		nations.put("affiliation", doc.select("stdyDscr dataAccs useStmt contact").attr("affiliation"));
		nations.put("uri", doc.select("stdyDscr dataAccs useStmt contact").attr("uri"));
		nations.put("email", doc.select("stdyDscr dataAccs useStmt contact").attr("email"));
		nations.put("contact", doc.select("stdyDscr dataAccs useStmt contact").text());
		return nations;
	}
	
	/**
	 * Under tag : stdyDscr > dataAccs > useStmt > citReq
	 */
	public String getCitReq() {
		return doc.select("stdyDscr dataAccs useStmt citReq").text();
	}
	
	/**
	 * Under tag : stdyDscr > dataAccs > useStmt > disclaimer
	 */
	public String getDisclaimer() {
		return doc.select("stdyDscr dataAccs useStmt disclaimer").text();
	}
	
	/**
	 * Under tag : fileDscr
	 */
	public List<Map<String, Object>> getFileDescription() {
		List<Map<String, Object>> descriptions = new ArrayList<>();
		Iterator<Element> it = doc.select("fileDscr").iterator();
		while(it.hasNext()) {
			Element el = it.next();
			
			Map<String, Object> description = new HashMap<>();
			description.put("id", el.attr("ID"));
			description.put("uri", el.attr("URI"));
			description.put("fileName", el.select("fileTxt fileName").text());
			description.put("fileCont", el.select("fileTxt fileCont").text());
			description.put("caseQnty", el.select("fileTxt dimensns caseQnty").text());
			description.put("varQnty", el.select("fileTxt dimensns varQnty").text());
			description.put("fileType", el.select("fileTxt fileType").text());
			descriptions.add(description);
		}
		
		return descriptions;
	}
	
	/**
	 * Under tag : dataDscr > var
	 */
	public List<Map<String, Object>> getVariables() {
		List<Map<String, Object>> descriptions = new ArrayList<>();
		Iterator<Element> it = doc.select("dataDscr var").iterator();
		while(it.hasNext()) {
			Element el = it.next();
			
			Map<String, Object> description = new HashMap<>();
			description.put("id", el.attr("ID"));
			description.put("name", el.attr("name"));
			description.put("file", el.attr("files"));
			description.put("decimal", el.attr("dcml"));
			description.put("interval", el.attr("intrvl"));
			description.put("label", el.select("labl").text());
			description.put("repondentUnit", el.select("respUnit").text());
			
			Map<String, Object> valRange = new HashMap<>();
			valRange.put("unit", el.select("valrng range").attr("UNITS"));
			valRange.put("min", el.select("valrng range").attr("min"));
			valRange.put("max", el.select("valrng range").attr("max"));
			description.put("valueRange", valRange);
			
			Map<String, Object> universe = new HashMap<>();
			valRange.put("clusion", el.select("universe").attr("clusion"));
			valRange.put("universe", el.select("universe").text());
			description.put("universe", universe);
			
			Map<String, Object> sumStat = new HashMap<>();
			Iterator<Element> elIt = el.select("sumStat").iterator();
			while(elIt.hasNext()) {
				Element sumStatEl = elIt.next();
				if(sumStatEl.attr("vald") != null || !sumStatEl.attr("vald").isEmpty()) 
					sumStat.put("valid", sumStatEl.text());
				if(sumStatEl.attr("invd") != null || !sumStatEl.attr("invd").isEmpty()) 
					sumStat.put("invalid", sumStatEl.text());
			}
			description.put("summaryStatistic", sumStat);
			
			List<Map<String, Object>> categories = new ArrayList<>();
			elIt = el.select("catgry").iterator();
			while(elIt.hasNext()) {
				Element categoryEl = elIt.next();
				Map<String, Object> category = new HashMap<>();
				category.put("value", categoryEl.select("catValu").text());
				category.put("label", categoryEl.select("labl").text());
				categories.add(category);
			}
			description.put("categories", categories);
			
			Map<String, Object> varFormat = new HashMap<>();
			valRange.put("type", el.select("varFormat").attr("type"));
			valRange.put("schema", el.select("varFormat").attr("schema"));
			description.put("variableFormat", varFormat);
			
			descriptions.add(description);
		}
		
		return descriptions;
	}
	
}
