package teddy.bigdawg.catalog;

public class CatalogUtilities {
	
	public static void checkConnection(Catalog cc) throws Exception {
		if (!cc.isInitiated()) 
			throw new Exception("[ERROR] BigDAWG: disconnected/uninitialized catalog.");
		if (!cc.isConnected()) {
			CatalogInitiator.connect(cc, cc.getLastURL(), cc.getLastUsername(), cc.getLastPassword());
		}
	}
	
	public static void checkLength(String s1, int len1) throws Exception{
		if (s1.length() > len1) 
			throw new Exception("[ERROR] BigDAWG: invalid length for \"" + s1 + "\";");
	}
}