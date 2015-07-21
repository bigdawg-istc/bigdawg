package istc.bigdawg.query.parser;

import java.util.HashSet;
import java.util.Stack;

import istc.bigdawg.BDConstants.Operator;
import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.Island;
import istc.bigdawg.exceptions.NotSupportIslandException;
import istc.bigdawg.query.ASTNode;
import istc.bigdawg.query.optimizer.SimpleShimSelector;

public class simpleParser implements Parser{
	public ASTNode parseQueryIntoTree(String text) throws NotSupportIslandException{
		int j=0;
		int k=0;
		int flag=0;
		HashSet<String> keywords = new HashSet<String>();
		keywords.add("RELATION");
		keywords.add("ARRAY");
		keywords.add("TEXT");
		keywords.add("STREAM");
		keywords.add("D4M");
		keywords.add("MYRIA");
		Stack<Character> e=new Stack<Character>();
		String subQuery="";
		Island island=Island.RELATION;
		Shim shim=Shim.PSQLRELATION;
		SimpleShimSelector shimSelector=new SimpleShimSelector();

		for(int i=0; i<text.length();i++){
			char a=text.charAt(i);
			switch(a){
			case ' ':
				j=i+1;
				break;
			case '(':
				e.push(a);
				if(keywords.contains(text.substring(j,i))){
					island=Island.get(text.substring(j,i));
					flag=e.size();
					k=i+1;
				}
				j=i+1;
				break;
			case ')':
				j=i+1;
				if(e.size()==flag){
					subQuery=text.substring(k,i);
				}

				e.pop();
				break;
			default:
				break;
			}

		}

		shim=shimSelector.selectShim(subQuery, island);
		ASTNode node = new ASTNode(subQuery, island, shim, Operator.SCOPE);
		return node;
	}
}
