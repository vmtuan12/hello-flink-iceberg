package org.tuan;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.flink.TableLoader;

public class RawStringToRowDataMapper implements MapFunction<String, RowData> {
    @Override
    public RowData map(String s) throws Exception {
        System.out.println("String value: " + s);
        String[] parts = s.split(",");

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, StringData.fromString(parts[0]));
        rowData.setField(1, StringData.fromString(parts[1]));
        if (parts.length > 2) {
            rowData.setRowKind(RowKind.DELETE);
        }

        System.out.println("Row data: " + rowData);
        return rowData;
    }
}
