package istc.bigdawg.catalog;

import java.sql.SQLException;

import istc.bigdawg.exceptions.BigDawgCatalogException;

public class CatalogUtilities {
	
	public static void checkConnection(Catalog cc) throws BigDawgCatalogException, SQLException {
		if (!cc.isInitiated()) 
			throw new BigDawgCatalogException("[ERROR] BigDAWG: disconnected/uninitialized catalog.");
		if (!cc.isConnected()) {
			CatalogInitiator.connect(cc, cc.getLastURL(), cc.getLastUsername(), cc.getLastPassword());
		}
	}
	
	public static void checkLength(String s1, int len1) throws BigDawgCatalogException {
		if (s1.length() > len1) 
			throw new BigDawgCatalogException("[ERROR] BigDAWG: invalid length for \"" + s1 + "\";");
	}
}