package org.embulk.parser.avro.getter;

import org.embulk.parser.avro.TimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampParser;

public class FloatColumnGetter extends BaseColumnGetter {
    protected Float value;

    private final TimestampUnit[] timestampUnits;

    public FloatColumnGetter(PageBuilder pageBuilder, TimestampParser[] timestampParsers, TimestampUnit[] timestampUnits) {
        super(pageBuilder, timestampParsers);
        this.timestampUnits = timestampUnits;
    }

    @Override
    public void setValue(Object value)
    {
        this.value = (Float) value;
    }

    @Override
    public void longColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            pageBuilder.setLong(column, value.longValue());
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
            pageBuilder.setString(column, value.toString());
        }
    }

    @Override
    public void timestampColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            TimestampUnit unit = timestampUnits[column.getIndex()];
            pageBuilder.setTimestamp(column, unit.toTimestamp(value.doubleValue()));
        }
    }
}
