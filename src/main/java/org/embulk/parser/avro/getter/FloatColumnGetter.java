package org.embulk.parser.avro.getter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class FloatColumnGetter extends BaseColumnGetter {

    public FloatColumnGetter(PageBuilder pageBuilder) {
        super(pageBuilder);
    }

    @Override
    public void longColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            Double casted = (Double) value;
            pageBuilder.setLong(column, casted.longValue());
        }
    }

    @Override
    public void doubleColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            Double casted = (Double) value;
            pageBuilder.setDouble(column, casted);
        }
    }

    @Override
    public void stringColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            Double casted = (Double) value;
            pageBuilder.setString(column, casted.toString());
        }
    }

    @Override
    public void timestampColumn(Column column) {

    }
}
