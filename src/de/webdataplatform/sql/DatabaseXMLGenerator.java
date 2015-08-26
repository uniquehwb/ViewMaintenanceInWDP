package de.webdataplatform.sql;

import java.util.List;

import java.io.File;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class DatabaseXMLGenerator {
	private List<Table> tables;

	public DatabaseXMLGenerator(List<Table> tables) {
		this.setTables(tables);
		generateXML();
	}

	public List<Table> getTables() {
		return tables;
	}

	public void setTables(List<Table> tables) {
		this.tables = tables;
	}

	private void generateXML() {
		try {

			DocumentBuilderFactory docFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

			// root elements
			Document doc = docBuilder.newDocument();
			Element rootElement = doc.createElement("setup");
			doc.appendChild(rootElement);

			Element dbSchema = doc.createElement("dbSchema");
			rootElement.appendChild(dbSchema);

			for (int i = 0; i < tables.size(); i++) {
				Element tableDefinition = doc.createElement("tableDefinition");
				dbSchema.appendChild(tableDefinition);
				
				Element name = doc.createElement("name");
				name.appendChild(doc.createTextNode(tables.get(i).getName()));
				tableDefinition.appendChild(name);
				
				Element primaryKey = doc.createElement("primaryKey");
				tableDefinition.appendChild(primaryKey);
				
				Element prefix = doc.createElement("prefix");
				prefix.appendChild(doc.createTextNode("k"));
				primaryKey.appendChild(prefix);
				
				Element startRange = doc.createElement("startRange");
				startRange.appendChild(doc.createTextNode("1"));
				primaryKey.appendChild(startRange);
				
				Element endRange = doc.createElement("endRange");
				endRange.appendChild(doc.createTextNode("1001"));
				primaryKey.appendChild(endRange);
				
				if ("basetable".equals(tables.get(i).getType())) {
					Element column1 = doc.createElement("column");
					tableDefinition.appendChild(column1);
					
					Element columnName1 = doc.createElement("name");
					columnName1.appendChild(doc.createTextNode("colAggKey"));
					column1.appendChild(columnName1);
					
					Element columnFamily1 = doc.createElement("family");
					columnFamily1.appendChild(doc.createTextNode("colfam1"));
					column1.appendChild(columnFamily1);
					
					Element columnPrefix1 = doc.createElement("prefix");
					columnPrefix1.appendChild(doc.createTextNode("x"));
					column1.appendChild(columnPrefix1);
					
					Element columnStartRange1 = doc.createElement("startRange");
					columnStartRange1.appendChild(doc.createTextNode("1"));
					column1.appendChild(columnStartRange1);
					
					Element columnEndRange1 = doc.createElement("endRange");
					columnEndRange1.appendChild(doc.createTextNode("1001"));
					column1.appendChild(columnEndRange1);
					
					Element column2 = doc.createElement("column");
					tableDefinition.appendChild(column2);
					
					Element columnName2 = doc.createElement("name");
					columnName2.appendChild(doc.createTextNode("colAggVal"));
					column2.appendChild(columnName2);
					
					Element columnFamily2 = doc.createElement("family");
					columnFamily2.appendChild(doc.createTextNode("colfam1"));
					column2.appendChild(columnFamily2);
					
					Element columnPrefix2 = doc.createElement("prefix");
					columnPrefix2.appendChild(doc.createTextNode(""));
					column2.appendChild(columnPrefix2);
					
					Element columnStartRange2 = doc.createElement("startRange");
					columnStartRange2.appendChild(doc.createTextNode("1"));
					column2.appendChild(columnStartRange2);
					
					Element columnEndRange2 = doc.createElement("endRange");
					columnEndRange2.appendChild(doc.createTextNode("101"));
					column2.appendChild(columnEndRange2);
				}
				
				if (tables.get(i).getBaseTables() != null) {
					for (int j = 0; j < tables.get(i).getBaseTables().size(); j++) {
						Element column3 = doc.createElement("column");
						tableDefinition.appendChild(column3);
						
						Element columnName3 = doc.createElement("name");
						columnName3.appendChild(doc.createTextNode("colAggVal" + (j+1)));
						column3.appendChild(columnName3);
						
						Element columnFamily3 = doc.createElement("family");
						columnFamily3.appendChild(doc.createTextNode("colfam" + (j+1)));
						column3.appendChild(columnFamily3);
						
						Element columnPrefix3 = doc.createElement("prefix");
						columnPrefix3.appendChild(doc.createTextNode(""));
						column3.appendChild(columnPrefix3);
						
						Element columnStartRange3 = doc.createElement("startRange");
						columnStartRange3.appendChild(doc.createTextNode("1"));
						column3.appendChild(columnStartRange3);
						
						Element columnEndRange3 = doc.createElement("endRange");
						columnEndRange3.appendChild(doc.createTextNode("101"));
						column3.appendChild(columnEndRange3);
					}
				}
			}

			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory
					.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(doc);
			
			StreamResult result = new StreamResult(new File("VMDatabaseConfig.xml"));

			// Output to console for testing
			// StreamResult result = new StreamResult(System.out);

			transformer.transform(source, result);

			System.out.println("XML file created!");

		} catch (ParserConfigurationException pce) {
			pce.printStackTrace();
		} catch (TransformerException tfe) {
			tfe.printStackTrace();
		}
	}
}
