package de.webdataplatform.sql;

import java.util.List;
import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class EvaluationXMLGenerator {

	private List<Table> tables;

	public EvaluationXMLGenerator(List<Table> tables)
			throws TransformerException {
		this.setTables(tables);
		generateXML();
	}

	public List<Table> getTables() {
		return tables;
	}

	public void setTables(List<Table> tables) {
		this.tables = tables;
	}

	private void generateXML() throws TransformerException {
		try {

			String filepath = "VMEvaluationConfig.xml";
			DocumentBuilderFactory docFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			Document doc = docBuilder.parse(filepath);

			// Get the createtables element by tag name directly
			Element oldCreatetables = (Element) doc.getElementsByTagName("createtables").item(0);
			// Remove old createtables node
			oldCreatetables.getParentNode().removeChild(oldCreatetables);
			
			Element experiment = (Element) doc.getElementsByTagName("experiment").item(0);
			Element createtables = doc.createElement("createtables");
			experiment.appendChild(createtables);

			// append a new node to staff
			for (int i = 0; i < tables.size(); i++) {
				Table table = tables.get(i);
				if ("basetable".equals(table.getType())) {
					Element basetable = doc.createElement("basetable");
					createtables.appendChild(basetable);

					Element name = doc.createElement("name");
					name.appendChild(doc.createTextNode(table.getName()));
					basetable.appendChild(name);

					Element numOfRegions = doc.createElement("numOfRegions");
					numOfRegions.appendChild(doc.createTextNode("5"));
					basetable.appendChild(numOfRegions);

					Element numOfOperations = doc
							.createElement("numOfOperations");
					numOfOperations.appendChild(doc.createTextNode("101"));
					basetable.appendChild(numOfOperations);

					Element distribution = doc.createElement("distribution");
					distribution.appendChild(doc.createTextNode("uniform"));
					basetable.appendChild(distribution);

					Element useUpdates = doc.createElement("useUpdates");
					useUpdates.appendChild(doc.createTextNode("false"));
					basetable.appendChild(useUpdates);

					Element useDeletes = doc.createElement("useDeletes");
					useDeletes.appendChild(doc.createTextNode("false"));
					basetable.appendChild(useDeletes);

				} else if ("delta".equals(table.getType())) {
					Element deltaView = doc.createElement("deltaView");
					createtables.appendChild(deltaView);

					Element name = doc.createElement("name");
					name.appendChild(doc.createTextNode(table.getName()));
					deltaView.appendChild(name);

					Element type = doc.createElement("type");
					type.appendChild(doc.createTextNode(table.getType()));
					deltaView.appendChild(type);

					Element basetable = doc.createElement("basetable");
					basetable.appendChild(doc.createTextNode(table
							.getBaseTables().get(0).getName()));
					deltaView.appendChild(basetable);

					Element numOfRegions = doc.createElement("numOfRegions");
					numOfRegions.appendChild(doc.createTextNode("2"));
					deltaView.appendChild(numOfRegions);

					Element columns = doc.createElement("columns");
					deltaView.appendChild(columns);

					Element column1 = doc.createElement("column");
					column1.appendChild(doc.createTextNode("colAggKey"));
					columns.appendChild(column1);

					Element column2 = doc.createElement("column");
					column2.appendChild(doc.createTextNode("colAggVal"));
					columns.appendChild(column2);

				} else if ("selection".equals(table.getType())) {
					Element selectionView = doc.createElement("selectionView");
					createtables.appendChild(selectionView);

					Element name = doc.createElement("name");
					name.appendChild(doc.createTextNode(table.getName()));
					selectionView.appendChild(name);

					Element basetable = doc.createElement("basetable");
					basetable.appendChild(doc.createTextNode(table
							.getBaseTables().get(0).getName()));
					selectionView.appendChild(basetable);

					Element selectionKey = doc.createElement("selectionKey");
					selectionKey.appendChild(doc.createTextNode("colAggVal"));
					selectionView.appendChild(selectionKey);

					Element selectionOperation = doc
							.createElement("selectionOperation");
					selectionOperation.appendChild(doc.createTextNode("&gt;"));
					selectionView.appendChild(selectionOperation);

					Element selectionValue = doc
							.createElement("selectionValue");
					selectionValue.appendChild(doc.createTextNode("10"));
					selectionView.appendChild(selectionValue);

					Element numOfRegions = doc.createElement("numOfRegions");
					numOfRegions.appendChild(doc.createTextNode("2"));
					selectionView.appendChild(numOfRegions);

				} else if ("sum".equals(table.getType())) {
					Element aggregationView = doc
							.createElement("aggregationView");
					createtables.appendChild(aggregationView);

					Element name = doc.createElement("name");
					name.appendChild(doc.createTextNode(table.getName()));
					aggregationView.appendChild(name);

					Element type = doc.createElement("type");
					type.appendChild(doc.createTextNode(table.getType()));
					aggregationView.appendChild(type);

					Element basetable = doc.createElement("basetable");
					basetable.appendChild(doc.createTextNode(table
							.getBaseTables().get(0).getName()));
					aggregationView.appendChild(basetable);

					Element aggregationKey = doc
							.createElement("aggregationKey");
					aggregationKey.appendChild(doc.createTextNode("colAggKey"));
					aggregationView.appendChild(aggregationKey);

					Element aggregationValue = doc
							.createElement("aggregationValue");
					aggregationValue.appendChild(doc
							.createTextNode("colAggVal"));
					aggregationView.appendChild(aggregationValue);

					Element numOfRegions = doc.createElement("numOfRegions");
					numOfRegions.appendChild(doc.createTextNode("2"));
					aggregationView.appendChild(numOfRegions);
				}
			}

			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory
					.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(doc);
			StreamResult result = new StreamResult(new File(filepath));
			transformer.transform(source, result);

			System.out.println("XML file modified!");

		} catch (ParserConfigurationException pce) {
			pce.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (SAXException sae) {
			sae.printStackTrace();
		}
	}
}
