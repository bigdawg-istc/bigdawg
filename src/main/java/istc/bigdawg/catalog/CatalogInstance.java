/**
 * 
 */
package istc.bigdawg.catalog;

import org.apache.log4j.Logger;

import istc.bigdawg.postgresql.PostgreSQLInstance;

/**
 * @author Adam Dziedzic
 * 
 * Singleton of the catalog instance.
 *
 */
public enum CatalogInstance {

	INSTANCE;
	
	private Logger logger = org.apache.log4j.Logger.getLogger(CatalogInstance.class
			.getName());
	private Catalog catalog;

	private CatalogInstance() {
		catalog = new Catalog();
		try {
			CatalogInitiator.connect(catalog, PostgreSQLInstance.URL, PostgreSQLInstance.USER,
					PostgreSQLInstance.PASSWORD);
		} catch (Exception e) {
			String msg = "Catalog initialization failed!";
			System.err.println(msg);
			logger.error(msg);
			e.printStackTrace();
			System.out.println("==>> " + PostgreSQLInstance.URL);
			System.out.println("==>> " + PostgreSQLInstance.USER);
			System.out.println("==>> " + PostgreSQLInstance.PASSWORD);
			System.exit(1);
		}
	}
	
	public Catalog getCatalog() {
		return catalog;
	}
	
	public void closeCatalog() {
		try {
			CatalogInitiator.close(catalog);
		} catch (Exception e) {
			String msg="Catalog closing failed!";
			System.err.println(msg);
			logger.error(msg);
			e.printStackTrace();
			System.exit(1);
		}
	}
}
