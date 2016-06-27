package istc.bigdawg.islands.PostgreSQL;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import istc.bigdawg.islands.PostgreSQL.utils.SQLPrepareQuery;
import istc.bigdawg.islands.PostgreSQL.utils.SQLUtilities;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;

public class SQLQueryPerformance {

	static Map<String, Map<String, List<String>>> performanceInfos = new HashMap<>();
	
	int skipSortCount = 0;
	int indentation = 0;
	int maxIndex = -1;
	Select select = null;
	
	public SQLQueryPerformance(Select select ) {
		this.select = select;
	}
	
	
	public String parseXMLString(String xmlString) throws Exception {

		DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		InputSource is = new InputSource();
		is.setCharacterStream(new StringReader(xmlString));
		Document document = builder.parse(is);
		  
	    //Iterating through the nodes and extracting the data.
        NodeList nodeList = document.getDocumentElement().getChildNodes();
      
		// <explain>            
	    for (int i = 0; i < nodeList.getLength(); i++) {
            Node query = nodeList.item(i);
            // <query>
            if(query.getNodeName() == "Query") {
            	for(int j = 0; j < query.getChildNodes().getLength(); ++j) {
            		Node plan = query.getChildNodes().item(j);
        		    // <Plan>
            		if(plan.getNodeName() == "Plan") {
            			parsePlanTail("main", plan, 0, false);
//            			break;
            		} else if (plan.getNodeName() == "Planning-Time" || plan.getNodeName() == "Execution-Time") {
        				print(plan.getNodeName() + ": " + plan.getTextContent(), 0, true);
        				if (plan.getNodeName() == "Execution-Time") 
        					break;
            		}
            	}
            }
           
	    }
			
	    return null;   
	}
	
	
	
	void parsePlanTail(String planName, Node node, int recursionLevel, boolean skipSort) throws Exception {
		
		if(node.getNodeName() != "Plan") {
			throw new Exception("Not parsing a valid plan node!");
		}

		boolean localSkipSort = false;
		
		List<String> sortKeys = new ArrayList<String>();
		List<String> outItems = new ArrayList<String>();
		Map<String, String> parameters = new HashMap<String, String>();
		String nodeType = null;
		String localPlan = planName;

		
		NodeList children = node.getChildNodes();
	
		List<Operator> childOps = new ArrayList<Operator>();
		
		// if node type matches scan then we have reached a leaf in the query plan
		// scan might be seq, cte, index, etc.
		
		String currentNodeName = null;
		
		for(int j = 0; j < children.getLength(); ++j) {
			Node c = children.item(j);
	
			switch(c.getNodeName())  {
			
			case "Node-Type":
				nodeType = c.getTextContent();
				parameters.put("Node-Type", nodeType);
				
				currentNodeName = nodeType;
				
				if (!performanceInfos.containsKey(currentNodeName))
					performanceInfos.put(currentNodeName, new HashMap<>());
				
				
//				print(nodeType, recursionLevel, false);
				
				if(nodeType.equals("Merge Join")) {
					
					localSkipSort = determineLocalSortSkip(planName);
					
				}
				break;
	
			case "Strategy":
				if(c.getTextContent().equals("Sorted") && nodeType.equals("Aggregate")) {
					localSkipSort = true;
				}
			
			case "Output":
				  NodeList output = c.getChildNodes();
				  for(int k = 0; k < output.getLength(); ++k) {
					  Node outExpr = output.item(k);
					  if(outExpr.getNodeName() == "Item") {
						  String s = SQLUtilities.removeOuterParens(outExpr.getTextContent());
						  outItems.add(s);
					  }
				  }
				  break;
			case "Sort-Key":
				  NodeList sortNodes = c.getChildNodes();
				  for(int k = 0; k < sortNodes.getLength(); ++k) {
					  Node outExpr = sortNodes.item(k);
					  if(outExpr.getNodeName() == "Item") {
						  String s = SQLUtilities.removeOuterParens(outExpr.getTextContent());
						  sortKeys.add(s);
					  }
				  }
				  
				  break;
			case "Subplan-Name":
				localPlan = c.getTextContent(); // switch to new CTE
				localPlan = localPlan.substring(localPlan.indexOf(" ")+1);  // chop out "CTE " prefix
				break;
			case "Plans":
				int r = recursionLevel+1;
				childOps = parsePlansTail(localPlan, c, r, localSkipSort);
				break;
			
			case "Startup-Cost":
			case "Total-Cost":
			case "Plan-Rows":
			case "Plan-Width":
			case "Actual-Startup-Time":
			case "Actual-Total-Time":
			case "Actual-Rows":
			case "Actual-Loops":
			case "Relation-Name":
				
				if (!performanceInfos.get(currentNodeName).containsKey(c.getNodeName()))
					performanceInfos.get(currentNodeName).put(c.getNodeName(), new ArrayList<>());
				
				performanceInfos.get(currentNodeName).get(c.getNodeName()).add(c.getTextContent());
				
			
//				print(c.getNodeName() + ": " + c.getTextContent(), recursionLevel, true);
				break;
			
				
			default:
				parameters.put(c.getNodeName(), c.getTextContent()); // record it for later
			}
	
			
		} // end children for plan

		

//		Operator op;
		if(nodeType.equals("Sort") && skipSort) { // skip sort associated with GroupAggregate
//			op = childOps.get(0);
			--skipSortCount;
		}
		else {
			parameters.put("sectionName", planName);
//			op =  OperatorFactory.get(nodeType, parameters, outItems, sortKeys, childOps, queryPlan, supplement);
		}

		
		if(!localPlan.equals(planName)) { // we created a cte
//			op.setCTERootStatus(true);
//			queryPlan.addPlan(localPlan, op);
		}
		
//		return op;
	}
	
	private boolean determineLocalSortSkip (String planName) {
		if (planName.equals("main")) {
	//		if (((PlainSelect) query.getSelectBody()).getOrderByElements() == null // || ((PlainSelect) query.getSelectBody()).getOrderByElements().isEmpty()
	//			) {
			return true;
	//		} 
		} else {
			if (select.getWithItemsList() != null //&& (!query.getWithItemsList().isEmpty())
					) {
				for (WithItem w : select.getWithItemsList()) {
					if (w.getName().equals(planName)) {
						if (((PlainSelect)w.getSelectBody()).getOrderByElements() == null // || ((PlainSelect) query.getSelectBody()).getOrderByElements().isEmpty()
								) {
							return true;
						}
					}
				}
			} 
		}
		
		return false;
	}
	
	// handle a <Plans> op, might return a list
	List<Operator> parsePlansTail(String planName, Node node, int recursionLevel, boolean skipSort) throws Exception {
		NodeList children = node.getChildNodes();
		List<Operator> childNodes = new ArrayList<Operator>();
		
		for(int i = 0; i < children.getLength(); ++i) {
			Node c = children.item(i);
			if(c.getNodeName() == "Plan") {
				
				parsePlanTail(planName, c, recursionLevel+1, skipSort);
				
				// only add children that are part of the main plan, not the CTEs which are accounted for in CTEScan
//				if(!o.CTERoot()) {
//					childNodes.add(o);
//				}
			}
		}
		
		return childNodes;
	}
	
	public static String getXMLStringWithPerformance(PostgreSQLHandler psqlh, String query) throws Exception {

		String explainQuery = SQLPrepareQuery.generateExplainQueryStringWithPerformance(query);
		String xmlString = psqlh.generatePostgreSQLQueryXML(explainQuery);
		
//		System.out.println("query: \n"+query);
//		System.out.println("\n\nXMLString: \n"+xmlString+"\n");
		
		return xmlString;
	}
	
	
	private void print(String s, int recLevel, boolean dashes) {
		
		for (int i = 0; i < 2*recLevel; i++)
			System.out.print(' ');
		
		if (dashes) 
			System.out.print("- ");
		
		System.out.println(s);
		
	}
	

}
