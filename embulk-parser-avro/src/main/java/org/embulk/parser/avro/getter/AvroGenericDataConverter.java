package org.embulk.parser.avro.getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public class AvroGenericDataConverter {
  public static Value convert(Object genericData) {
    return toValue(genericData);
  }

  private static Value toValue(Object rawValue) {
    if (rawValue instanceof Utf8) {
      return ValueFactory.newString(rawValue.toString());
    } else if (rawValue instanceof Integer) {
      return ValueFactory.newInteger((Integer) rawValue);
    } else if (rawValue instanceof Long) {
      return ValueFactory.newInteger((Long) rawValue);
    } else if (rawValue instanceof Float) {
      return ValueFactory.newFloat((Float) rawValue);
    } else if (rawValue instanceof Double) {
      return ValueFactory.newFloat((Double) rawValue);
    } else if (rawValue instanceof Boolean) {
      return ValueFactory.newBoolean((Boolean) rawValue);
    } else if (rawValue instanceof GenericData.EnumSymbol) {
      return ValueFactory.newString(rawValue.toString());
    } else if (rawValue instanceof GenericData.Array) {
      List<Value> list = new ArrayList<>();
      for (Object item : (GenericData.Array) rawValue) {
        list.add(toValue(item));
      }
      return ValueFactory.newArray(list);
    } else if (rawValue instanceof GenericData.Record) {
      Map<Value, Value> map = new HashMap<>();
      GenericData.Record casted = (GenericData.Record) rawValue;
      for (Schema.Field field : casted.getSchema().getFields()) {
        Object val = casted.get(field.name());
        Value keyValue = ValueFactory.newString(field.name());
        Value valValue = toValue(val);
        map.put(keyValue, valValue);
      }
      return ValueFactory.newMap(map);
    } else if (rawValue instanceof HashMap) {
      Map<Value, Value> map = new HashMap<>();
      HashMap casted = (HashMap) rawValue;
      Set entries = casted.entrySet();
      for (Object entry : entries) {
        Map.Entry et = (Map.Entry) entry;
        Utf8 key = (Utf8) (et.getKey());
        Object val = et.getValue();
        Value keyValue = toValue(key);
        Value valValue = toValue(val);
        map.put(keyValue, valValue);
      }
      return ValueFactory.newMap(map);
    } else if (rawValue == null) {
      return ValueFactory.newNil();
    } else {
      throw new RuntimeException("Unknown type");
    }
  }
}
