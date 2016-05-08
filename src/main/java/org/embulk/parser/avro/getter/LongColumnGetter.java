package org.embulk.parser.avro.getter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class LongColumnGetter extends BaseColumnGetter {
    protected Long value;

    public LongColumnGetter(PageBuilder pageBuilder) {
        super(pageBuilder);
    }

    @Override
    public void setValue(Object value)
    {
        this.value = (Long) value;
    }

    @Override
    public void longColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            pageBuilder.setLong(column, value);
        }
    }

    @Override
    public void doubleColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            pageBuilder.setDouble(column, value.doubleValue());
        }
    }

    @Override
    public void stringColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            Long casted = (Long) value;
            pageBuilder.setString(column, value.toString());
        }
    }
}
