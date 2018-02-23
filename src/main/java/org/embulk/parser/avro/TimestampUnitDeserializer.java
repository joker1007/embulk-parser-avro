package org.embulk.parser.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;

public class TimestampUnitDeserializer extends FromStringDeserializer<TimestampUnit>
{

    public static ImmutableMap<String, TimestampUnit> mapping = ImmutableMap.<String, TimestampUnit>builder()
            .put("Second", TimestampUnit.Second)
            .put("second", TimestampUnit.Second)
            .put("sec", TimestampUnit.Second)
            .put("s", TimestampUnit.Second)
            .put("MilliSecond", TimestampUnit.MilliSecond)
            .put("millisecond", TimestampUnit.MilliSecond)
            .put("milli_second", TimestampUnit.MilliSecond)
            .put("milli", TimestampUnit.MilliSecond)
            .put("msec", TimestampUnit.MilliSecond)
            .put("ms", TimestampUnit.MilliSecond)
            .put("MicroSecond", TimestampUnit.MicroSecond)
            .put("microsecond", TimestampUnit.MicroSecond)
            .put("micro_second", TimestampUnit.MicroSecond)
            .put("micro", TimestampUnit.MicroSecond)
            .put("usec", TimestampUnit.MicroSecond)
            .put("us", TimestampUnit.MicroSecond)
            .put("NanoSecond", TimestampUnit.NanoSecond)
            .put("nanosecond", TimestampUnit.NanoSecond)
            .put("nano_second", TimestampUnit.NanoSecond)
            .put("nano", TimestampUnit.NanoSecond)
            .put("nsec", TimestampUnit.NanoSecond)
            .put("ns", TimestampUnit.NanoSecond)
            .build();

    public TimestampUnitDeserializer() {
        super(TimestampUnit.class);
    }

    @Override
    protected TimestampUnit _deserialize(String value, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        TimestampUnit unit = mapping.get(value);
        if (unit == null) {
            throw new JsonMappingException(
                    String.format("Unknown type name '%s'. Supported types are: %s",
                            value,
                            Joiner.on(", ").join(mapping.keySet())));
        }
        return unit;
    }
}
