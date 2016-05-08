require 'avro'
require 'time'

file = File.open('items.avro', 'wb')

schema = Avro::Schema.parse(File.open("item.avsc", "rb").read)

writer = Avro::IO::DatumWriter.new(schema)

dw = Avro::DataFile::Writer.new(file, writer, schema)

dw << {
  "id" => 1,
  "code" => 123456789012345678,
  "name" => "Desktop",
  "description" => "Office and Personal Usage",
  "flag" => true,
  "created_at" => Time.now.iso8601,
  "created_at_utc" => Time.now.to_f,
  "spec" => {"key" => "opt1", "value" => "optvalue1"},
  "tags" => ["tag1", "tag2"],
  "price" => 30000,
  "options" => {"foo" => "bar", "hoge" => nil},
  "item_type" => "D",
  "dummy" => nil,
}
dw << {
  "id" => 2,
  "code" => 123456789012345679,
  "name" => "Laptop",
  "flag" => false,
  "created_at" => Time.now.iso8601,
  "created_at_utc" => Time.now.to_f,
  "spec" => {"key" => "opt1", "value" => nil},
  "price" => 50000,
  "options" => {},
  "item_type" => "M",
}
dw << {
  "id" => 3,
  "code" => 123456789012345680,
  "name" => "Tablet",
  "description" => "Personal Usage",
  "flag" => true,
  "created_at" => Time.now.iso8601,
  "created_at_utc" => Time.now.to_f,
  "tags" => ["tag3"],
  "spec" => {"key" => "opt1", "value" => "optvalue1"},
  "options" => {},
  "item_type" => "M",
}
dw << {
  "id" => 4,
  "code" => 123456789012345681,
  "name" => "Mobile",
  "description" => "Personal Usage",
  "flag" => true,
  "created_at" => Time.now.iso8601,
  "created_at_utc" => Time.now.to_f,
  "spec" => {"key" => "opt1", "value" => "optvalue1"},
  "tags" => [],
  "price" => 10000,
  "options" => {},
  "item_type" => "M",
}
dw << {
  "id" => 5,
  "code" => 123456789012345682,
  "name" => "Notepad",
  "flag" => true,
  "created_at" => Time.now.iso8601,
  "created_at_utc" => Time.now.to_f,
  "spec" => {"key" => "opt1", "value" => "optvalue1"},
  "tags" => ["tag1", "tag2"],
  "price" => 20000,
  "options" => {},
  "item_type" => "M",
}
dw << {
  "id" => 6,
  "code" => 123456789012345683,
  "name" => "SmartPhone",
  "description" => "Multipurpose",
  "flag" => true,
  "created_at" => Time.now.iso8601,
  "created_at_utc" => Time.now.to_f,
  "spec" => {"key" => "opt1", "value" => "optvalue1"},
  "tags" => ["tag1", "tag2"],
  "price" => 40000,
  "options" => {},
  "item_type" => "M",
}

dw.close
