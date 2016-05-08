package org.embulk.parser.avro.getter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampParser;

public class BooleanColumnGetter extends BaseColumnGetter {
    protected Boolean value;

    public BooleanColumnGetter(PageBuilder pageBuilder, TimestampParser[] timestampParsers) {
        super(pageBuilder, timestampParsers);
    }

    @Override
    public void setValue(Object value)
    {
        this.value = (Boolean) value;
    }

    @Override
    public void booleanColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            pageBuilder.setBoolean(column, value);
        }
    }

    @Override
    public void stringColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            pageBuilder.setString(column, value.toString());
        }
    }
}
