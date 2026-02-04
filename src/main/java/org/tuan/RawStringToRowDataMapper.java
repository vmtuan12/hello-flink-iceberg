package org.tuan;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

public class RawStringToRowDataMapper implements MapFunction<String, RowData> {
    @Override
    public RowData map(String s) throws Exception {
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, StringData.fromString(s));
        return rowData;
    }
}
