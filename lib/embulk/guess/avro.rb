module Embulk
  module Guess
    class Avro < GuessPlugin
      Plugin.register_guess("avro", self)

      def guess_columns(sample_buffer)
        classpath = File.expand_path('../../../../classpath', __FILE__)
        Dir["#{classpath}/*.jar"].each {|jar| require jar }
        java_import org.apache.avro.file.DataFileReader
        java_import org.apache.avro.file.SeekableByteArrayInput
        java_import org.apache.avro.generic.GenericDatumReader
        java_import org.embulk.spi.type.Types

        def convert_type(field)
          type_map = {
            org.apache.avro.Schema::Type::STRING  => Types::STRING.getName,
            org.apache.avro.Schema::Type::BYTES   => Types::STRING.getName,
            org.apache.avro.Schema::Type::FIXED   => Types::STRING.getName,
            org.apache.avro.Schema::Type::ENUM    => Types::STRING.getName,
            org.apache.avro.Schema::Type::NULL    => Types::STRING.getName,
            org.apache.avro.Schema::Type::INT     => Types::LONG.getName,
            org.apache.avro.Schema::Type::LONG    => Types::LONG.getName,
            org.apache.avro.Schema::Type::FLOAT   => Types::DOUBLE.getName,
            org.apache.avro.Schema::Type::DOUBLE  => Types::DOUBLE.getName,
            org.apache.avro.Schema::Type::BOOLEAN => Types::BOOLEAN.getName,
            org.apache.avro.Schema::Type::MAP     => Types::JSON.getName,
            org.apache.avro.Schema::Type::ARRAY   => Types::JSON.getName,
            org.apache.avro.Schema::Type::RECORD  => Types::JSON.getName,
          }
          type = field.schema.getType
          if type == org.apache.avro.Schema::Type::UNION
            type = field.schema.getTypes.find {|e|
              e.getType != org.apache.avro.Schema::Type::NULL
            }.getType
          end
          type_map[type]
        end

        sbai = SeekableByteArrayInput.new(sample_buffer.to_java_bytes)
        reader = DataFileReader.new(sbai, GenericDatumReader.new)
        schema = reader.getSchema
        schema.getFields.map {|f| {name: f.name, type: convert_type(f)}}
      end

      def guess(config, sample_buffer)
        if sample_buffer[0,4] == "Obj\x01"
          guessed = {}
          guessed["type"] = "avro"
          guessed["columns"] = guess_columns(sample_buffer)
          return {"parser" => guessed}
        else
          return {}
        end
      end
    end
  end
end
