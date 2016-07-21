/**
 * 
 */
package istc.bigdawg.migration;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NoTargetArrayException;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.SessionIdentifierGenerator;
import istc.bigdawg.utils.StackTrace;

/**
 * Export data from SciDB.
 * 
 * @author Adam Dziedzic
 */
public class ExportSciDB implements Export {

	/**
	 * Determines if a de-serialized file is compatible with this class.
	 */
	private static final long serialVersionUID = -5120857056647396576L;

	/* log */
	private static Logger log = Logger.getLogger(LoadSciDB.class);

	/**
	 * Arrays in SciDB (from which we export the data): either multi-dimensional
	 * or flat array.
	 */
	private SciDBArrays arrays;

	/**
	 * The format in which data should be written to the file/pipe/output
	 * stream.
	 */
	private FileFormat fileFormat;

	/**
	 * The full path to the output file - where the data should be extracted to.
	 */
	private String outputFile = null;

	/**
	 * The full specification of the binary format e.g.: (int32, string, string)
	 */
	private String binFullFormat;

	/**
	 * Information about the migration process.
	 * 
	 * {@link #setMigrationInfo(MigrationInfo)}
	 */
	private MigrationInfo migrationInfo = null;

	/** Handler to the database to which we load the data. */
	@SuppressWarnings("unused")
	private transient DBHandler handlerTo;

	/*
	 * These are the intermediate (additional) arrays that were created during
	 * migration of data between PostgreSQL and SciDB. If something fails on the
	 * way or at the end of the migration process, the arrays should be removed.
	 * 
	 * On the other hand, the tables in PostgreSQL are created within a
	 * transaction so if something goes wrong in PostgreSQL, then the database
	 * itself takes care of cleaning the created but not loaded tables.
	 */
	private Set<String> intermediateArrays = new HashSet<>();

	/**
	 * Declare only the file format in which the data should be exported. The
	 * remaining parameters should be added when the migration is prepared.
	 * 
	 * @param fileFormat
	 *            File format in which the data should be exported.
	 */
	private ExportSciDB(FileFormat fileFormat) {
		this.fileFormat = fileFormat;
	}

	/**
	 * see: {@link #ExportSciDB(FileFormat)}
	 * 
	 * @param fileFormat
	 * @return Instance of ExportSciDB which will export data in the specified
	 *         fileFormat.
	 */
	public static ExportSciDB ofFormat(FileFormat fileFormat) {
		return new ExportSciDB(fileFormat);
	}

	public ExportSciDB(MigrationInfo migrationInfo, SciDBArrays arrays,
			String scidbFilePath, FileFormat fileFormat, String binFullFormat) {
		this.migrationInfo = migrationInfo;
		this.arrays = arrays;
		this.outputFile = scidbFilePath;
		this.fileFormat = fileFormat;
		this.binFullFormat = binFullFormat;
	}

	/**
	 * Example of the binary format for SciDB: (string, int64, int64 null)
	 * 
	 * @return the string representing a binary format for SciDB
	 * @throws NoTargetArrayException
	 * @throws SQLException
	 * @throws MigrationException
	 */
	public static String getSciDBBinFormat(MigrationInfo migrationInfo,
			String array) throws NoTargetArrayException, SQLException,
					MigrationException {
		SciDBHandler handler = new SciDBHandler(
				migrationInfo.getConnectionFrom());
		SciDBArrayMetaData arrayMetaData;
		try {
			arrayMetaData = handler.getObjectMetaData(array);
		} catch (Exception e) {
			throw new MigrationException(e.getMessage(), e);
		} finally {
			handler.close();
		}
		List<AttributeMetaData> attributes = arrayMetaData
				.getAttributesOrdered();
		StringBuilder binBuf = new StringBuilder();
		for (AttributeMetaData attribute : attributes) {
			binBuf.append(attribute.getSqlDataType());
			if (attribute.isNullable()) {
				binBuf.append(" null");
			}
			binBuf.append(",");
		}
		// remove the last comma ,
		binBuf.deleteCharAt(binBuf.length() - 1);
		return binBuf.toString();
	}

	/**
	 * Load the data to SciDB (identified by connectionTo): to a given array
	 * from a given file.
	 * 
	 * @param connectionTo
	 * @param arrays
	 * @param outputFile
	 * @return
	 * @throws SQLException
	 */
	public Object call() throws MigrationException {
		StringBuilder saveCommand = new StringBuilder();
		String saveCommandFinal = null;
		if (fileFormat == FileFormat.CSV) {
			String csvFormat = null;
			String array = migrationInfo.getObjectFrom();
			if (arrays == null) {
				/*
				 * We assume that the source array is always multi-dimensional.
				 */
				arrays = new SciDBArrays(null,
						new SciDBArray(array, false, false));
			}
			if (arrays.getMultiDimensional() != null) {
				array = arrays.getMultiDimensional().getName();
				csvFormat = "csv+";
			}
			/* this is only a flat array so export only the attributes */
			else if (arrays.getFlat() != null) {
				/* for flat array we export only the attributes */
				csvFormat = "csv";
				array = arrays.getFlat().getName();
			} else {
				throw new IllegalStateException("Either a multi-dimensional or "
						+ "a flat array has to be specified for SciDB export.");
			}
			saveCommandFinal = "save(" + array + ",'" + outputFile + "',-2,'"
					+ csvFormat + "')";
		} else if (fileFormat == FileFormat.BIN_SCIDB) {
			String array = null;
			if (arrays == null) {
				String newFlatIntermediateArray = array + "__bigdawg__flat__"
						+ SessionIdentifierGenerator.INSTANCE
								.nextRandom26CharString();
				try {
					SciDBHandler.createFlatArrayFromMultiDimArray(
							migrationInfo.getConnectionFrom(),
							migrationInfo.getObjectFrom(),
							newFlatIntermediateArray);
				} catch (Exception e) {
					throw new MigrationException(e.getMessage(), e);
				}
				intermediateArrays.add(newFlatIntermediateArray);
				arrays = new SciDBArrays(
						new SciDBArray(newFlatIntermediateArray, true, true),
						new SciDBArray(array, false, false));
				try {
					binFullFormat = ExportSciDB.getSciDBBinFormat(migrationInfo,
							newFlatIntermediateArray);
				} catch (NoTargetArrayException | SQLException e) {
					throw new MigrationException(e.getMessage(), e);
				}
			}
			if (arrays.getMultiDimensional() != null) {
				String multiDimArray = arrays.getMultiDimensional().getName();
				String flatArray = arrays.getFlat().getName();
				array = "store(redimension(" + multiDimArray + "," + flatArray
						+ ")," + flatArray + ")";
			} else {
				/* only the flat array */
				array = arrays.getFlat().getName();
			}
			saveCommand.append("save(" + array + ", '" + outputFile + "'");
			saveCommand.append(",-2,'");

			saveCommand.append("(" + binFullFormat + ")");
			saveCommand.append("')");
			saveCommandFinal = saveCommand.toString();
		} else {
			throw new IllegalArgumentException("The file format: " + fileFormat
					+ " is not supported for export from SciDB.");
		}
		log.debug("save command: " + LogUtils.replace(saveCommandFinal));
		SciDBHandler handler;
		try {
			handler = new SciDBHandler(migrationInfo.getConnectionFrom());
			handler.executeStatementAFL(saveCommandFinal);
			handler.commit();
			handler.close();
		} catch (SQLException e) {
			log.error(e.getMessage() + StackTrace.getFullStackTrace(e));
			throw new MigrationException(e.getMessage(), e);
		}
		MigrationUtils.removeArrays(migrationInfo.getConnectionFrom(),
				"clean the intermediate arrays", intermediateArrays);
		String message = "Data successfuly exported from SciDB";
		log.debug(message);
		/*
		 * SciDB does not return the information how many elements were
		 * exported.
		 */
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * istc.bigdawg.migration.ConnectorChecker#isSupportedConnector(istc.bigdawg
	 * .query.ConnectionInfo)
	 */
	@Override
	public boolean isSupportedConnector(ConnectionInfo connection) {
		if (connection instanceof SciDBConnectionInfo) {
			return true;
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * istc.bigdawg.migration.SetMigrationInfo#setMigrationInfo(istc.bigdawg.
	 * migration.MigrationInfo)
	 */
	@Override
	public void setMigrationInfo(MigrationInfo migrationInfo) {
		this.migrationInfo = migrationInfo;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.migration.Export#setExportTo(java.lang.String)
	 */
	@Override
	public void setExportTo(String filePath) {
		this.outputFile = filePath;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.migration.Export#getHandler()
	 */
	@Override
	public DBHandler getHandler() throws MigrationException {
		try {
			return new SciDBHandler(migrationInfo.getConnectionFrom());
		} catch (SQLException e) {
			throw new MigrationException(
					e.getMessage() + " Cannot instantiate the SciDBHandler.");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * istc.bigdawg.migration.Export#setHandlerTo(istc.bigdawg.query.DBHandler)
	 */
	@Override
	public void setHandlerTo(DBHandler handlerTo) throws MigrationException {
		this.handlerTo = handlerTo;
	}

}
