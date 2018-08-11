package istc.bigdawg.rest;

import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.database.ObjectMetaData;
import istc.bigdawg.utils.Tuple;

import java.util.ArrayList;
import java.util.List;

public class RESTObjectMetaData implements ObjectMetaData {
    private List<Tuple.Tuple3<String, String, Boolean>> headers;

    RESTObjectMetaData(List<Tuple.Tuple3<String, String, Boolean>> headers) {
        this.headers = headers;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public List<AttributeMetaData> getAttributesOrdered() {
        List <AttributeMetaData> resultList = new ArrayList<>();
        if (headers == null) {
            AttributeMetaData attribute = new AttributeMetaData("col1", "json", true, false);
            resultList.add(attribute);
            return resultList;
        }

        int pos = 0;
        for(Tuple.Tuple3<String, String, Boolean> header: headers) {
            AttributeMetaData attribute = new AttributeMetaData(header.getT1(), pos, header.getT2(), header.getT3(), false);
            resultList.add(attribute);
            pos++;
        }
        return resultList;
    }
}
