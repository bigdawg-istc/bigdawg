package istc.bigdawg.signature;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import costmodel.StringUnitCostModel;
import distance.APTED;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.islands.IslandAndCastResolver;
import istc.bigdawg.islands.IslandAndCastResolver.Scope;
import istc.bigdawg.islands.QueryContainerForCommonDatabase;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.relational.utils.SQLExpressionUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import node.Node;
import node.StringNodeData;
import parser.BracketStringInputParser;

public class Signature {
	private static Logger logger = Logger.getLogger(Signature.class.getName());
	
	private static String fieldSeparator = "|||||";
	private static String fieldSeparatorRest = "[|][|][|][|][|]";
	private static String elementSeparator = "&&&&&";
	private static String elementSeparatorRest = "[&][&][&][&][&]";
	
	private Scope island;
	private String sig1;
	private List<String> sig2;
	private List<String> sig3;
	private String query;
	private List<String> sig4k;
	private List<Map<String, Set<String>>> objectExpressionMapping = null;
//	private String identifier; 
	
//	private static Pattern possibleObjectsPattern	= Pattern.compile("[_@a-zA-Z0-9]+");
//	private static Pattern tagPattern				= Pattern.compile("BIGDAWGTAG_[0-9_]+");
	
	/**
	 * Construct a signature of three parts: sig1 tells about the structure, sig2 are all object references, sig3 are all constants
	 * @param cc
	 * @param query
	 * @param island
	 * @throws Exception
	 */
	public Signature(String query, Scope island, Operator root, Map<String, QueryContainerForCommonDatabase> container, Set<String> joinPredicates) throws Exception {

//		setSig3(TheObjectThatResolvesAllDifferencesAmongTheIslands.getLiteralsAndConstantsSignature(island, query));
		setSig3(IslandAndCastResolver.getIsland(island).getLiteralsAndConstantsSignature(query));
		
		objectExpressionMapping = new ArrayList<>();
		if (container.isEmpty() ) {
			Map<String, Set<String>> mapping = root.getObjectToExpressionMappingForSignature();
			root.removeCTEEntriesFromObjectToExpressionMapping(mapping);
			objectExpressionMapping.add(mapping);
		}
		
		List<String> cs = new ArrayList<>();
		for (String s : container.keySet()) {
			cs.add(container.get(s).generateTreeExpression());
			objectExpressionMapping.add(container.get(s).generateObjectToExpressionMapping());
		}
		setSig4k(cs);
		setSig1(root.getTreeRepresentation(true));
		
		this.setQuery(query);
		this.setIsland(island);

		List<String> predicates = new ArrayList<>();
		predicates.addAll(joinPredicates);
		setSig2(predicates);
	}
	
	
	public Signature(String s) throws BigDawgException {

		List<String> parsed = Arrays.asList(s.split(fieldSeparatorRest));
		if (parsed.size() != 5 && parsed.size() != 6) {
			throw new BigDawgException("Ill-formed input string; cannot recover signature; String: "+s);
		}
		try {
			this.island = Scope.valueOf(parsed.get(0));
			this.sig1 = new String(parsed.get(1));
			this.sig2 = Arrays.asList(parsed.get(2).split(elementSeparatorRest));
			this.sig3 = Arrays.asList(parsed.get(3).split(elementSeparatorRest));
			this.query = new String(parsed.get(4));
			if (parsed.size() == 5)
				this.sig4k = new ArrayList<>();
			else
				this.sig4k = Arrays.asList(parsed.get(5).split(elementSeparatorRest));
		} catch (Exception e) {
			e.printStackTrace();
			throw new BigDawgException("Ill-formed input string; cannot recover signature; String: "+s);
		}
	}
	
	public static double getTreeEditDistance(String s1, String s2) {
		
	    BracketStringInputParser parser = new BracketStringInputParser();
	    Node<StringNodeData> t1 = parser.fromString(s1);
	    Node<StringNodeData> t2 = parser.fromString(s2);
	    // Initialise APTED.
	    APTED<StringUnitCostModel, StringNodeData> apted = new APTED<>(new StringUnitCostModel());
	    // Although we don't need TED value yet, TED must be computed before the
	    // mapping. This cast is safe due to unit cost.
	    apted.computeEditDistance(t1, t2);
	    // Get TED value corresponding to the computed mapping.
	    LinkedList<int[]> mapping = apted.computeEditMapping();
	    // This cast is safe due to unit cost.
	    return apted.mappingCost(mapping);
	}
	
	@Override
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		sb.append("Signature:\n");
		sb.append("Island       : ").append(island.toString()).append('\n');
		sb.append("Signature 1  : ").append(sig1.toString()).append('\n');
		sb.append("Signature 2  : ").append(sig2.toString()).append('\n');
		sb.append("Signature 3  : ").append(sig3.toString()).append('\n');
		sb.append("Query        : ").append(query).append('\n');
		sb.append("Signature 4-k: ").append(sig4k.toString()).append('\n');
		
		return sb.toString();
	}
	public void print() {
		System.out.println(this.toString());
	}
	
	public String getSig1() {
		return sig1;
	}

	public void setSig1(String sig1) {
		this.sig1 = sig1;
	}

	public List<String> getSig2() {
		return sig2;
	}

	public void setSig2(List<String> sig2) {
		this.sig2 = sig2;
	}

	public List<String> getSig3() {
		return sig3;
	}

	public void setSig3(List<String> sig3) {
		this.sig3 = sig3;
	}

	public String getQuery() throws IslandException {
		return IslandAndCastResolver.getIsland(island).wrapQueryInIslandIdentifier(query);
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public Scope getIsland() {
		return island;
	}
	
	private void setIsland(Scope island) {
		this.island = island;
	}

	public List<String> getSig4k() {
		return sig4k;
	}

	public void setSig4k(List<String> sig4k) {
		this.sig4k = sig4k;
	}
	
	public List<Map<String, Set<String>>> getObjectToExpressionMapping() {
		return objectExpressionMapping;
	}
	
	public List<Map<String, Set<String>>> getTreesOfObjectToExpressionMapping() throws JSQLParserException {
		
		List<Map<String, Set<String>>> ret = new ArrayList<>();
		for (Map<String, Set<String>> mapping : objectExpressionMapping) {
			Map<String, Set<String>> addition = new HashMap<>();
			for (String obj : mapping.keySet()) {
				Set<String> expr = new HashSet<>();
				for (String exp : mapping.get(obj)) {
					expr.add(SQLExpressionUtils.parseCondForTree(CCJSqlParserUtil.parseCondExpression(exp)));
				}
				addition.put(obj, expr);
			}
			ret.add(addition);
		}
		
		return ret;
	}
	
	public List<Map<String, Set<String>>> getTreesOfStrippedDownObjectToExpressionMapping() throws JSQLParserException {
		
		List<Map<String, Set<String>>> ret = new ArrayList<>();
		for (Map<String, Set<String>> mapping : objectExpressionMapping) {
			Map<String, Set<String>> addition = new HashMap<>();
			for (String obj : mapping.keySet()) {
				Set<String> expr = new HashSet<>();
				for (String exp : mapping.get(obj)) {
					
					Expression e = SQLExpressionUtils.stripDownExpressionForSignature(CCJSqlParserUtil.parseCondExpression(exp));
					while (e instanceof Parenthesis) e = ((Parenthesis)e).getExpression();
					if (e instanceof Column) continue;
					
					expr.add(SQLExpressionUtils.parseCondForTree(e));
				}
				addition.put(obj, expr);
			}
			ret.add(addition);
		}
		
		return ret;
	}
	
	public static void printO2EMapping(Operator o) throws Exception {
		Map<String, Set<String>> m = o.getObjectToExpressionMappingForSignature();
		o.removeCTEEntriesFromObjectToExpressionMapping(m);
		System.out.println("Mapping: ");
		for (String s : m.keySet()) {
			System.out.printf("-- %s:\n",s);
			for (String s2 : m.get(s)) {
				String e;
				try {
					e = SQLExpressionUtils.parseCondForTree(CCJSqlParserUtil.parseCondExpression(s2));
				} catch (JSQLParserException ex) {
					e = SQLExpressionUtils.parseCondForTree(CCJSqlParserUtil.parseExpression(s2));
				}
				System.out.printf("  - %s\n",e);
			}
		}
	}
	
	public static void printStrippedO2EMapping(Operator o) throws Exception {
		Map<String, Set<String>> m = o.getObjectToExpressionMappingForSignature();
		o.removeCTEEntriesFromObjectToExpressionMapping(m);
		System.out.println("Stripped down function: ");
		for (String s : m.keySet()) {
			System.out.printf("-- %s:\n",s);
			for (String s2 : m.get(s)) {
				Expression e;
				try {
					e = SQLExpressionUtils.stripDownExpressionForSignature(CCJSqlParserUtil.parseCondExpression(s2));
				} catch (JSQLParserException ex) {
					e = SQLExpressionUtils.stripDownExpressionForSignature(CCJSqlParserUtil.parseExpression(s2));
				}
				while (e instanceof Parenthesis) e = ((Parenthesis)e).getExpression();
				if (e instanceof Column) continue;
				System.out.printf("  - %s\n",SQLExpressionUtils.parseCondForTree(e));
			}
		}
	}
	
	public String toRecoverableString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(island.toString());
		sb.append(fieldSeparator).append(sig1);
		sb.append(fieldSeparator).append(String.join(elementSeparator, sig2));
		sb.append(fieldSeparator).append(String.join(elementSeparator, sig3));
		sb.append(fieldSeparator).append(query);
		
		if (sig4k.size() > 0)
			sb.append(fieldSeparator).append(String.join(elementSeparator, sig4k));
		
		return sb.toString();
	}
	
	
	public double compare(Signature sig) {
		logger.debug("SIGNATURE 1: " + this.toRecoverableString());
		logger.debug("SIGNATURE 2: " + sig.toRecoverableString());

		double dist = 0;

		double sig1Weight = 2;
		double sig2Weight = 1;
		double sig3Weight = 1;
		double sig4kWeight = 2;

		// sig1
		double treeEdit1 = getTreeEditDistance(sig1, "{}");
		double treeEdit2 = getTreeEditDistance(sig.sig1, "{}");
		double sig1Max = treeEdit1 > treeEdit2 ? treeEdit1 : treeEdit2;
		double sig1Dist = getTreeEditDistance(sig1, sig.sig1);
		sig1Dist /= sig1Max;
		logger.debug("SIGNATURE sig1 dist: " + sig1Dist);

		// sig2
		List<String> l2;
		double sig2Max = sig2.size() > sig.sig2.size() ? sig2.size() : sig.sig2.size();
		if (sig2.size() > sig.sig2.size()) {
			l2 = new ArrayList<>(sig2);
			l2.retainAll(sig.sig2);
		} else { 
			l2 = new ArrayList<>(sig.sig2);
			l2.retainAll(sig2);
		}
		double sig2Dist = sig2Max - (double)l2.size();
		sig2Dist /= sig2Max;
		logger.debug("SIGNATURE sig2 dist: " + sig2Dist);
		
		// sig3
		double sig3Max = (sig3.size() > sig.sig3.size()) ? sig3.size() : sig.sig3.size();
		double sig3Dist = (sig3.size() > sig.sig3.size()) ? sig3.size() - sig.sig3.size() : sig.sig3.size() - sig3.size();
		sig3Dist /= sig3Max;
		logger.debug("SIGNATURE sig3 dist: " + sig3Dist);
		
		// sig4k
		List<String> l4k2 = new ArrayList<>(sig.sig4k);
		double sig4kMax = sig4k.size() > sig.sig4k.size() ? sig4k.size() : sig.sig4k.size();
		double sig4kDist = sig4k.size() < sig.sig4k.size() ? sig.sig4k.size() - sig4k.size() : sig4k.size() - sig.sig4k.size();
		double tree4k1 = 0.0;
		double tree4k2 = 0.0;

		for (String aSig4k: l4k2){
			tree4k2 += getTreeEditDistance(aSig4k, "{}");
		}

		for (String aSig4k : sig4k) {
			tree4k1 += getTreeEditDistance(aSig4k, "{}");
			double result = Double.MAX_VALUE;
			int j = 0;
			int holder = -1;
			while (!l4k2.isEmpty() && j < l4k2.size()) {
				double temp = getTreeEditDistance(aSig4k, l4k2.get(j));
				if (temp < result) {
					result = temp;
					holder = j;
				}
				j++;
			}
			if (holder > 0) {
				l4k2.remove(holder);
				sig4kDist += result;
			} else
				break;
		}
		sig4kMax += tree4k1 > tree4k2 ? tree4k1 : tree4k2;
		sig4kDist /= sig4kMax;
		logger.debug("SIGNATURE sig4k dist: " + sig4kDist);

		dist += sig1Dist*sig1Weight + sig2Dist*sig2Weight + sig3Dist*sig3Weight + sig4kDist*sig4kWeight;
		dist /= sig1Weight + sig2Weight + sig3Weight + sig4kWeight;
		logger.debug("SIGNATURE final dist: " + dist);
		return dist;
	}
	
	
	
}