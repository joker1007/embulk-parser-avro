package org.embulk.parser.avro.getter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class StringColumnGetter extends BaseColumnGetter {

    public StringColumnGetter(PageBuilder pageBuilder) {
        super(pageBuilder);
    }

    @Override
    public void booleanColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setBoolean(column, Boolean.parseBoolean(this.value.toString()));
        }
    }

    @Override
    public void longColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setLong(column, Long.parseLong(this.value.toString()));
        }
    }

    @Override
    public void doubleColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setDouble(column, Double.parseDouble(this.value.toString()));
        }
    }

    @Override
    public void stringColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setString(column, this.value.toString());
        }
    }

    @Override
    public void timestampColumn(Column column) {

    }

    @Override
    public void jsonColumn(Column column) {

    }
}
