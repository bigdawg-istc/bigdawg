package istc.bigdawg.plan.extract;

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

import istc.bigdawg.extract.logical.SQLParseLogical;
import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.SQLQueryPlan;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.plan.operators.OperatorFactory;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.utils.sqlutil.SQLPrepareQuery;
import istc.bigdawg.utils.sqlutil.SQLUtilities;
//import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;


// takes in a psql plan from running
// EXPLAIN (VERBOSE ON, FORMAT XML) SELECT ...
// Produces a set of operators and their source / destination schemas
// see codegen.ops for tree nodes

// first pass, just parse ops - arrange for bottom up analysis
// build one obj per SELECT block, build tree to link them together
// second pass - map back to schema

public class SQLPlanParser {
	
	// needed for type resolution
	//private DatabaseSingleton catalog;
	SQLQueryPlan queryPlan;
	
	int skipSortCount = 0;
	
	// NEW
	Select query;

    
	// sqlPlan passes in supplement info
	public SQLPlanParser(String xmlString, SQLQueryPlan sqlPlan, String q) throws Exception {
	   //catalog = DatabaseSingleton.getInstance();
		
		String qprocessed = SQLPrepareQuery.preprocessDateAndTime(q);
		this.query = (Select) CCJSqlParserUtil.parse(qprocessed);
		queryPlan = sqlPlan;
		
		Map<String, String> extraInformation = new HashMap<>();
		if (((PlainSelect)this.query.getSelectBody()).getLimit() != null) {
			Limit lim = ((PlainSelect)this.query.getSelectBody()).getLimit();
			if (lim.isLimitAll()) extraInformation.put("LimitAll", "ALL");
			else if (lim.isLimitNull()) extraInformation.put("LimitNull", "NULL");
			else extraInformation.put("LimitCount", Long.toString(lim.getRowCount()));
			if (lim.getOffset() > 0) extraInformation.put("LimitOffset", Long.toString(lim.getOffset()));
			
		}
		
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
            			Operator root = parsePlanTail("main", plan, extraInformation, 0, false);
            			queryPlan.setRootNode(root); 

            			break;
            		}
            	}
            }
           
	    }
		
	    // resolve any aggregate in where
//	    queryPlan.getRootNode().seekScanAndProcessAggregateInFilter();
//	    for (String s : queryPlan.getPlanRoots().keySet())
//	    	queryPlan.getPlanRoot(s).seekScanAndProcessAggregateInFilter();
		    
	}
	
	public static SQLQueryPlan extract(String sql) throws Exception {

		String xml = sql.replace(".sql", ".xml");
		
		SQLPrepareQuery.generatePlan(sql, xml);
		
		// set up supplement
		String query = SQLPrepareQuery.readSQL(sql);
		SQLParseLogical parser = new SQLParseLogical(query);
		SQLQueryPlan queryPlan = parser.getSQLQueryPlan();
		
		// run parser
		@SuppressWarnings("unused")
		SQLPlanParser p = new SQLPlanParser(xml, queryPlan, query);
		
		return queryPlan;
	}
	
	public static SQLQueryPlan extractDirect(PostgreSQLHandler psqlh, String query) throws Exception {

		String explainQuery = SQLPrepareQuery.generateExplainQueryString(query);
		String xmlString = psqlh.generatePostgreSQLQueryXML(explainQuery);
		
		// set up supplement
		SQLParseLogical parser = new SQLParseLogical(query);
		SQLQueryPlan queryPlan = parser.getSQLQueryPlan();
		
		
		System.out.println("query: \n"+query);
		System.out.println("\n\nXMLString: \n"+xmlString+"\n");
		
		// run parser
		@SuppressWarnings("unused")
		SQLPlanParser p = new SQLPlanParser(xmlString, queryPlan, query);
		
		return queryPlan;
	}
	
	// parse a single <Plan>
	Operator parsePlanTail(String planName, Node node, Map<String, String> extraInformation, int recursionLevel, boolean skipSort) throws Exception {
		
		if(node.getNodeName() != "Plan") {
			throw new Exception("Not parsing a valid plan node!");
		}

		boolean localSkipSort = false;
		
		List<String> sortKeys = new ArrayList<String>();
		List<String> outItems = new ArrayList<String>();
		Map<String, String> parameters = new HashMap<String, String>();
		String nodeType = null;
		String localPlan = planName;
		SQLTableExpression supplement = queryPlan.getSQLTableExpression(planName);

		
		NodeList children = node.getChildNodes();
	
		List<Operator> childOps = new ArrayList<Operator>();
		
		// if node type matches scan then we have reached a leaf in the query plan
		// scan might be seq, cte, index, etc.
		
		for(int j = 0; j < children.getLength(); ++j) {
			Node c = children.item(j);
	
			switch(c.getNodeName())  {
			
			case "Node-Type":
				nodeType = c.getTextContent();
				parameters.put("Node-Type", nodeType);
				if(nodeType.equals("Merge Join")) {
					localSkipSort = determineLocalSortSkip(planName);
				} else if (nodeType.equals("Limit")) {
					parameters.putAll(extraInformation);
				}
				break;
	
			case "Strategy":
				if(c.getTextContent().equals("Sorted") && nodeType.equals("Aggregate") && (supplement.getOrderByClause() == null || supplement.hasOuterSortBeenProcessed())) {
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
				  supplement.setOuterSortProcesssed(true);
				  break;
			case "Subplan-Name":
				localPlan = c.getTextContent(); // switch to new CTE
				localPlan = localPlan.substring(localPlan.indexOf(" ")+1);  // chop out "CTE " prefix
				supplement = queryPlan.getSQLTableExpression(localPlan);
				break;
			case "Plans":
				int r = recursionLevel+1;
				childOps = parsePlansTail(localPlan, c, extraInformation, r, localSkipSort);
				break;
				
			default:
				parameters.put(c.getNodeName(), c.getTextContent()); // record it for later
			}
	
			
		} // end children for plan

		

		Operator op;
		if(nodeType.equals("Sort") && skipSort) { // skip sort associated with GroupAggregate
			op = childOps.get(0);
			--skipSortCount;
		}
		else {
			parameters.put("sectionName", planName);
			op =  OperatorFactory.get(nodeType, parameters, outItems, sortKeys, childOps, queryPlan, supplement);
		}

		
		if(!localPlan.equals(planName)) { // we created a cte
			op.setCTERootStatus(true);
			queryPlan.addPlan(localPlan, op);
		}
		
		return op;
	}
	
	/**
	 * This is used to determine whether a merge-sort should let its child skip its sort should it sees one
	 * @param planName
	 * @return
	 */
	private boolean determineLocalSortSkip (String planName) {
		if (planName.equals("main")) {
//			if (((PlainSelect) query.getSelectBody()).getOrderByElements() == null // || ((PlainSelect) query.getSelectBody()).getOrderByElements().isEmpty()
//				) {
			return true;
//			} 
		} else {
			if (query.getWithItemsList() != null //&& (!query.getWithItemsList().isEmpty())
					) {
				for (WithItem w : query.getWithItemsList()) {
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
	List<Operator> parsePlansTail(String planName, Node node, Map<String, String> extraInformation, int recursionLevel, boolean skipSort) throws Exception {
		NodeList children = node.getChildNodes();
		List<Operator> childNodes = new ArrayList<Operator>();
		
		for(int i = 0; i < children.getLength(); ++i) {
			Node c = children.item(i);
			if(c.getNodeName() == "Plan") {
				Operator o = parsePlanTail(planName, c, extraInformation, recursionLevel+1, skipSort);
				
				// only add children that are part of the main plan, not the CTEs which are accounted for in CTEScan
				if(!o.CTERoot()) {
					childNodes.add(o);
				}
			}
		}
		
		return childNodes;
	}

	
//	public static String padLeft(String s, int n) {
//		if(n > 0) {
//			return String.format("%1$" + n + "s", s);  
//		}
//		 
//		return s;
//	}
//
//	public static String padRight(String s, int n) {
//		if(n > 0) {
//		     return String.format("%1$-" + n + "s", s);  
//		}
//		return s;
//		
//	}
	
}

	
