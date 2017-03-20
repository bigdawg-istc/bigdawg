package istc.bigdawg.vertica;

import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.relational.RelationalTableMetaData;

import java.util.List;
import java.util.Map;

/**
 * @author Kate Yu
 */
public class VerticaTableMetaData implements RelationalTableMetaData {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /** Name of the schema (by default public) and name of the table. */
    private VerticaSchemaTableName schemaTable;
    private Map<String, AttributeMetaData> columnsMap;
    private List<AttributeMetaData> columnsOrdered;

    public VerticaTableMetaData(VerticaSchemaTableName schemaTable,
                              Map<String, AttributeMetaData> columnsMap,
                              List<AttributeMetaData> columnsOrdered) {
        this.schemaTable = schemaTable;
        this.columnsMap = columnsMap;
        this.columnsOrdered = columnsOrdered;
    }

    /**
     * @return the schemaTable
     */
    public VerticaSchemaTableName getSchemaTable() {
        return schemaTable;
    }

    public Map<String, AttributeMetaData> getColumnsMap() {
        return columnsMap;
    }

    @Override
    public String getName() {
        return getSchemaTable().getFullName();
    }

    @Override
    public List<AttributeMetaData> getAttributesOrdered() {
        return columnsOrdered;
    }
}
