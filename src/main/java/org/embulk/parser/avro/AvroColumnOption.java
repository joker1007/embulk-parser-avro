package org.embulk.parser.avro;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import org.embulk.spi.type.Type;

import com.google.common.base.Optional;

public interface AvroColumnOption
        extends Task
{
    @Config("type")
    @ConfigDefault("null")
    Optional<Type> getType();
}
