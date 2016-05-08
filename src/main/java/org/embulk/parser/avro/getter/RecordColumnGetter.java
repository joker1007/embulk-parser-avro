package org.embulk.parser.avro.getter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampParser;
import org.msgpack.value.Value;

public class RecordColumnGetter extends BaseColumnGetter {
    public RecordColumnGetter(PageBuilder pageBuilder, TimestampParser[] timestampParsers) {
        super(pageBuilder, timestampParsers);
    }

    @Override
    public void stringColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            Value converted = AvroGenericDataConverter.convert(value);
            pageBuilder.setString(column, converted.toString());
        }
    }

    @Override
    public void jsonColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            Value converted = AvroGenericDataConverter.convert(value);
            pageBuilder.setJson(column, converted);
        }
    }
}
