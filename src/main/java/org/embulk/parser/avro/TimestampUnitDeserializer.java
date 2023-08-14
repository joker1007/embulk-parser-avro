package org.embulk.parser.avro;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TimestampUnitDeserializer extends FromStringDeserializer<TimestampUnit> {

  public static final Map<String, TimestampUnit> MAPPING;

  static {
    MAPPING = new HashMap<>();
    MAPPING.put("Second", TimestampUnit.Second);
    MAPPING.put("second", TimestampUnit.Second);
    MAPPING.put("sec", TimestampUnit.Second);
    MAPPING.put("s", TimestampUnit.Second);
    MAPPING.put("MilliSecond", TimestampUnit.MilliSecond);
    MAPPING.put("millisecond", TimestampUnit.MilliSecond);
    MAPPING.put("milli_second", TimestampUnit.MilliSecond);
    MAPPING.put("milli", TimestampUnit.MilliSecond);
    MAPPING.put("msec", TimestampUnit.MilliSecond);
    MAPPING.put("ms", TimestampUnit.MilliSecond);
    MAPPING.put("MicroSecond", TimestampUnit.MicroSecond);
    MAPPING.put("microsecond", TimestampUnit.MicroSecond);
    MAPPING.put("micro_second", TimestampUnit.MicroSecond);
    MAPPING.put("micro", TimestampUnit.MicroSecond);
    MAPPING.put("usec", TimestampUnit.MicroSecond);
    MAPPING.put("us", TimestampUnit.MicroSecond);
    MAPPING.put("NanoSecond", TimestampUnit.NanoSecond);
    MAPPING.put("nanosecond", TimestampUnit.NanoSecond);
    MAPPING.put("nano_second", TimestampUnit.NanoSecond);
    MAPPING.put("nano", TimestampUnit.NanoSecond);
    MAPPING.put("nsec", TimestampUnit.NanoSecond);
    MAPPING.put("ns", TimestampUnit.NanoSecond);
  }

  public TimestampUnitDeserializer() {
    super(TimestampUnit.class);
  }

  @Override
  protected TimestampUnit _deserialize(String value, DeserializationContext ctxt)
      throws IOException {
    TimestampUnit unit = MAPPING.get(value);
    if (unit == null) {
      throw new JsonMappingException(
          String.format(
              "Unknown type name '%s'. Supported types are: %s",
              value, String.join(", ", MAPPING.keySet())));
    }
    return unit;
  }
}
