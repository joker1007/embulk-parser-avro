package org.embulk.parser.avro.getter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class IntegerColumnGetter extends BaseColumnGetter {
    public IntegerColumnGetter(PageBuilder pageBuilder) {
        super(pageBuilder);
    }

    @Override
    public void longColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            Long casted = (Long) value;
            pageBuilder.setLong(column, casted);
        }
    }

    @Override
    public void doubleColumn(Column column) {

    }

    @Override
    public void stringColumn(Column column) {

    }

    @Override
    public void timestampColumn(Column column) {

    }

    @Override
    public void jsonColumn(Column column) {

    }
}
