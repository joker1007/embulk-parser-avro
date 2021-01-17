package org.embulk.guess.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.GuessPlugin;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.util.config.ConfigMapperFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroGuessPlugin
        implements GuessPlugin {

    private static final byte[] AVRO_HEADER = {0x4f, 0x62, 0x6a, 0x01};

    private static final Map<Schema.Type, Type> TYPE_MAP = new EnumMap<>(Schema.Type.class);

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().build();

    static {
        TYPE_MAP.put(Schema.Type.STRING, Types.STRING);
        TYPE_MAP.put(Schema.Type.BYTES, Types.STRING);
        TYPE_MAP.put(Schema.Type.FIXED, Types.STRING);
        TYPE_MAP.put(Schema.Type.ENUM, Types.STRING);
        TYPE_MAP.put(Schema.Type.NULL, Types.STRING);
        TYPE_MAP.put(Schema.Type.INT, Types.LONG);
        TYPE_MAP.put(Schema.Type.LONG, Types.LONG);
        TYPE_MAP.put(Schema.Type.FLOAT, Types.DOUBLE);
        TYPE_MAP.put(Schema.Type.DOUBLE, Types.DOUBLE);
        TYPE_MAP.put(Schema.Type.BOOLEAN, Types.BOOLEAN);
        TYPE_MAP.put(Schema.Type.MAP, Types.JSON);
        TYPE_MAP.put(Schema.Type.ARRAY, Types.JSON);
        TYPE_MAP.put(Schema.Type.RECORD, Types.JSON);
    }

    private Type convertType(Schema.Field field) {
        Schema.Type type = field.schema().getType();
        if (type == Schema.Type.UNION) {
            for (Schema schema : field.schema().getTypes()) {
                Schema.Type t = schema.getType();
                if (t != Schema.Type.NULL) {
                    type = t;
                    break;
                }
            }
        }
        return TYPE_MAP.get(type);
    }

    private byte[] copyBuffer(Buffer buffer, int size) {
        byte[] bytes = new byte[size];
        buffer.getBytes(0, bytes, 0, size);
        return bytes;
    }

    @Override
    public ConfigDiff guess(ConfigSource config, Buffer sample) {
        ConfigDiff configDiff = CONFIG_MAPPER_FACTORY.newConfigDiff();

        byte[] bytes = copyBuffer(sample, AVRO_HEADER.length);
        if (!Arrays.equals(bytes, AVRO_HEADER)) {
            return configDiff;
        }
        ConfigDiff parserConfig = configDiff.set("parser", Collections.emptyMap()).getNested("parser");
        parserConfig.set("type", "avro");

        bytes = copyBuffer(sample, sample.capacity());
        DataFileReader<GenericRecord> reader;
        try {
            reader = new DataFileReader<>(new SeekableByteArrayInput(bytes), new GenericDatumReader<>());
        } catch (IOException e) {
            return configDiff;
        }
        List<Map<String, String>> columns = new ArrayList<>();
        for (Schema.Field field : reader.getSchema().getFields()) {
            Map<String, String> column = new HashMap<>();
            column.put("name", field.name());
            column.put("type", convertType(field).getName());
            columns.add(column);
        }
        parserConfig.set("columns", columns);

        return configDiff;
    }
}
