package org.embulk.parser.avro.getter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class StringColumnGetter extends BaseColumnGetter {
    protected String value;

    public StringColumnGetter(PageBuilder pageBuilder) {
        super(pageBuilder);
    }

    @Override
    public void setValue(Object value)
    {
        if (value == null)
            this.value = null;
        else
            this.value = value.toString();
    }

    @Override
    public void booleanColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setBoolean(column, Boolean.parseBoolean(value));
        }
    }

    @Override
    public void longColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setLong(column, Long.parseLong(value));
        }
    }

    @Override
    public void doubleColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setDouble(column, Double.parseDouble(value));
        }
    }

    @Override
    public void stringColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setString(column, value);
        }
    }

    @Override
    public void timestampColumn(Column column) {

    }

    @Override
    public void jsonColumn(Column column) {

    }
}
