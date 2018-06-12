package istc.bigdawg.cast;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import org.apache.log4j.Logger;

import istc.bigdawg.accumulo.AccumuloConnectionInfo;
import istc.bigdawg.accumulo.AccumuloMigrationParams;
import istc.bigdawg.catalog.CatalogModifier;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.exceptions.CastException;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.islands.CrossIslandCast;
import istc.bigdawg.islands.CrossIslandQueryNode;
import istc.bigdawg.islands.IntraIslandQuery;
import istc.bigdawg.islands.IslandAndCastResolver.Scope;
import istc.bigdawg.islands.text.operators.TextScan;
import istc.bigdawg.migration.MigrationParams;
import istc.bigdawg.migration.Migrator;
import istc.bigdawg.query.ConnectionInfo;

public class CastOverseer {

	private static Logger logger = Logger.getLogger(CastOverseer.class);
	
	public static boolean isCastAllowed(Scope source, Scope target) {
		//FIXME update catalog entries and implement this. Maybe for 0.2 release
		return true;
	}
	
	/**
	 * Executing cast, for Planner
	 * @param cast
	 * @param source
	 * @param target
	 * @param targetDBID
	 * @param connectionInfoMap
	 * @param tempTableInfo
	 * @return the object identity, oid, of intermediate result table on remote database
	 * @throws Exception
	 */
	public static int cast(
			CrossIslandCast cast,
			IntraIslandQuery source, 
			IntraIslandQuery target,
			Map<CrossIslandQueryNode, ConnectionInfo> connectionInfoMap, 
			Map<ConnectionInfo, Collection<String>> tempTableInfo) throws CastException {
		
		int sourceDBID = 0;
		int targetDBID = 0;
		
		if (target.getRemainderLoc() != null) {
			targetDBID = Integer.parseInt(target.getRemainderLoc().get(0));
		} else {
			try {
				targetDBID = Integer.parseInt((new ArrayList<>(target.getQueryContainer().entrySet())).get(0).getValue().getDBID());
			} catch (Exception e) {
				throw new CastException(e.getMessage(), e);
			}
		}

		// get the source, and get the engine 
		ConnectionInfo targetConnInfo = null;
		if (targetDBID > 0)
			try {
				targetConnInfo = CatalogViewer.getConnectionInfo(targetDBID);
			} catch (BigDawgCatalogException | SQLException e) {
				throw new CastException(e.getMessage(), e);
			}
		else
			throw new CastException(String.format("\n\nNegative target loc: %s; requires resolution.\n\n", targetDBID));
		
		String remoteName = processRemoteName(cast.getSourceScope(), cast.getDestinationScope(), cast.getName());

		logger.debug(String.format("\n\nconnectionInfoMap: %s; source: %s\n\n", connectionInfoMap, source));
		logger.debug(String.format("Interisland Migration from %s at %s (%s) to %s at %s (%s)"
				, source.getName()
				, connectionInfoMap.get(source).getHost() + ":" + connectionInfoMap.get(source).getPort()
				, connectionInfoMap.get(source).getClass().getSimpleName()
				, remoteName
				, targetConnInfo.getHost() + ":" + targetConnInfo.getPort()
				, targetConnInfo.getClass().getSimpleName()));
		logger.debug(String.format("CAST query string: %s", cast.getQueryString()));
		
		// migrate
		if (!tempTableInfo.containsKey(targetConnInfo)) {
			tempTableInfo.put(targetConnInfo, new HashSet<>());
		}
		tempTableInfo.get(targetConnInfo).add(remoteName);
		if (connectionInfoMap.get(source) instanceof AccumuloConnectionInfo) {
			TextScan ts = ((TextScan) source.getRemainder(0));
			logger.debug(String.format("Migrate from Accumulo: srcTbl: %s, rmtNm: %s, queryStr: %s, range: %s", 
					ts.getSourceTableName(), remoteName, cast.getQueryString(), ts.getRange()));
			try {
				Migrator.migrate(connectionInfoMap.get(source), ts.getSourceTableName(), //source.getName(), 
						targetConnInfo, remoteName, new AccumuloMigrationParams(cast.getQueryString(), ts.getRange()));
			} catch (MigrationException e) {
				throw new CastException(e.getMessage(), e);
			}
		} else {
			if (!tempTableInfo.containsKey(connectionInfoMap.get(source))) {
				tempTableInfo.put(connectionInfoMap.get(source), new HashSet<>());
			}
			tempTableInfo.get(connectionInfoMap.get(source)).add(source.getName());
			try {
				Migrator.migrate(connectionInfoMap.get(source), source.getName(), targetConnInfo, remoteName, new MigrationParams(cast.getQueryString(), source, target));
			} catch (MigrationException e) {
				throw new CastException(e.getMessage(), e);
			}
		}
		
		// TODO currently, sourceDBID is zero, 'token of indecision'. This needs fixing
		try {
			return CatalogModifier.addObject(remoteName, "", sourceDBID, targetDBID);
		} catch (BigDawgCatalogException | SQLException e) {
			throw new CastException(e.getMessage(), e);
		}
	}
	
private static String processRemoteName(Scope sourceScope, Scope destinationScope, String originalString) {
		
		if (sourceScope.equals(destinationScope)) 
			return originalString;
		if (sourceScope.equals(Scope.ARRAY)) 
			return originalString.replaceAll("___", ".");
		if (destinationScope.equals(Scope.ARRAY))
			return originalString.replaceAll("[.]", "___");
		
		return originalString;
	}
}
