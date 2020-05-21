package com.situation.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * 列 转 行
 */
public class ColumnToLine extends TableFunction<Row> {

    public ColumnToLine() {
    }

    public void eval(Row[] rows) {
     for (Row row:rows){
         collector.collect(Row.of(row.getField(0),row.getField(1)));
     }
    }

    @Override
    public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
        return new RowTypeInfo(Types.OBJECT_ARRAY(Types.ROW(Types.STRING, Types.STRING))).getFieldTypes();
    }


    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING, Types.STRING);
    }
}
