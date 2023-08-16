package org.embulk.parser.avro;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.embulk.spi.time.Timestamp;

@JsonDeserialize(using = TimestampUnitDeserializer.class)
public enum TimestampUnit {
  Second {
    @Override
    public Timestamp toTimestamp(Long value) {
      return Timestamp.ofEpochSecond(value);
    }

    @Override
    public Timestamp toTimestamp(Double value) {
      long sec = value.longValue();
      double rest = value - sec;
      return Timestamp.ofEpochSecond(0, sec * 1000000000L + (long) (rest * 1000000000L));
    }
  },
  MilliSecond {
    @Override
    public Timestamp toTimestamp(Long value) {
      return Timestamp.ofEpochSecond(0, value * 1000000L);
    }

    @Override
    public Timestamp toTimestamp(Double value) {
      long sec = value.longValue();
      double rest = value - sec;
      return Timestamp.ofEpochSecond(0, sec * 1000000L + (long) (rest * 1000000L));
    }
  },
  MicroSecond {
    @Override
    public Timestamp toTimestamp(Long value) {
      return Timestamp.ofEpochSecond(0, value * 1000L);
    }

    @Override
    public Timestamp toTimestamp(Double value) {
      long sec = value.longValue();
      double rest = value - sec;
      return Timestamp.ofEpochSecond(0, sec * 1000L + (long) (rest * 1000L));
    }
  },
  NanoSecond {
    @Override
    public Timestamp toTimestamp(Long value) {
      return Timestamp.ofEpochSecond(0, value);
    }

    @Override
    public Timestamp toTimestamp(Double value) {
      return Timestamp.ofEpochSecond(0, value.longValue());
    }
  };

  public abstract Timestamp toTimestamp(Long value);

  public abstract Timestamp toTimestamp(Double value);
}
