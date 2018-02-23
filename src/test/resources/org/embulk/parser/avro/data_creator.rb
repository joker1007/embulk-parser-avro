require 'avro'
require 'json'

schema = Avro::Schema.parse(File.read(ARGV[0]))
file = File.open(ARGV[1], 'wb')
writer = Avro::IO::DatumWriter.new(schema)
dw = Avro::DataFile::Writer.new(file, writer, schema)

data = File.read(ARGV[2]).each_line.map do |l|
  JSON.load(l)
end

data.each do |d|
  dw << d
end

dw.close
