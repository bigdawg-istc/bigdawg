/**
 * 
 */
package istc.bigdawg.accumulo;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * @author Adam Dziedzic 
 * 
 *
 */
public class AccumuloRowQualifier {
	private boolean isRowId;
	private boolean isColFam;
	private boolean isColQual;
	private boolean isValue;
	
	public class AccumuloRow {
		private Text rowId = new Text();
		private Text colFam = new Text();
		private Text colQual = new Text();
		private Value value = new Value();

		public AccumuloRow(Text rowId, Text colFam, Text colQual, Value value) {
			this.rowId=rowId;
			this.colFam=colFam;
			this.colQual=colQual;
			this.value=value;
		}

	/**
		 * @return the rowId
		 */
		public Text getRowId() {
			return rowId;
		}

		/**
		 * @param rowId the rowId to set
		 */
		public void setRowId(Text rowId) {
			this.rowId = rowId;
		}

		/**
		 * @return the colFam
		 */
		public Text getColFam() {
			return colFam;
		}

		/**
		 * @param colFam the colFam to set
		 */
		public void setColFam(Text colFam) {
			this.colFam = colFam;
		}

		/**
		 * @return the colQual
		 */
		public Text getColQual() {
			return colQual;
		}

		/**
		 * @param colQual the colQual to set
		 */
		public void setColQual(Text colQual) {
			this.colQual = colQual;
		}

		/**
		 * @return the value
		 */
		public Value getValue() {
			return value;
		}

		/**
		 * @param value the value to set
		 */
		public void setValue(Value value) {
			this.value = value;
		}
	}

	/**
	 * @param isRow
	 * @param isColFam
	 * @param isColName
	 * @param isValue
	 */
	public AccumuloRowQualifier(boolean isRowId, boolean isColFam, boolean isColQual,
			boolean isValue) {
		this.isRowId = isRowId;
		this.isColFam = isColFam;
		this.isColQual= isColQual;
		this.isValue = isValue;
	}

	public AccumuloRow getAccumuloRow(String[] tokens) {
		int index=0;
		Text rowId = new Text("");
		Text colFam = new Text("");
		Text colQual = new Text("");
		Value value = new Value("".getBytes());
		if (isRowId)
			rowId = new Text(tokens[index++]);
		if (isColFam)
			colFam = new Text(tokens[index++]);
		if (isColQual)
			colQual = new Text(tokens[index++]);
		if (isValue)
			value = new Value(tokens[index++].getBytes());
		return new AccumuloRow(rowId,colFam,colQual,value);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
