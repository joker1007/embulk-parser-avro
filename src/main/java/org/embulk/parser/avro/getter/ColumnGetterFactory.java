package org.embulk.parser.avro.getter;

import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;

public class ColumnGetterFactory {
    private org.apache.avro.Schema avroSchema;
    private PageBuilder pageBuilder;

    public ColumnGetterFactory(org.apache.avro.Schema avroSchema, PageBuilder pageBuilder)
    {
        this.avroSchema = avroSchema;
        this.pageBuilder = pageBuilder;
    }

    public BaseColumnGetter newColumnGetter(Column column)
    {
        org.apache.avro.Schema fieldSchema = avroSchema.getField(column.getName()).schema();
        switch (fieldSchema.getType().getName()) {
            case "union" :
                String typeName = "";
                for (org.apache.avro.Schema type : fieldSchema.getTypes()) {
                    if (!type.getName().equals("null")) {
                        typeName = type.getName();
                        break;
                    }
                }
                return getColumnGetterFromTypeName(typeName);
            default :
                return getColumnGetterFromTypeName(fieldSchema.getType().getName());
        }
    }

    private BaseColumnGetter getColumnGetterFromTypeName(String typeName)
    {
        switch (typeName) {
            case "string":
                return new StringColumnGetter(pageBuilder);
            case "int":
                return new IntegerColumnGetter(pageBuilder);
            case "long":
                return new IntegerColumnGetter(pageBuilder);
            case "float":
                return new FloatColumnGetter(pageBuilder);
            case "double":
                return new FloatColumnGetter(pageBuilder);
            case "byte":
            default:
                throw new DataException(String.format("%s is not supported", typeName));
        }
    }
}
