# Avro parser plugin for Embulk

[Avro](http://avro.apache.org/) parser plugin for Embulk.

## Overview

* **Plugin type**: parser
* **Guess supported**: no

## Configuration

- **type**: Specify this parser as avro
- **avsc**: Specify avro schema file. (required if `columns` is not specified)
- **columns**: Specify column name and type. See below (array, optional)
* **default_timezone**: Default timezone of the timestamp (string, default: UTC)
* **default_timestamp_format**: Default timestamp format of the timestamp (string, default: `%Y-%m-%d %H:%M:%S.%N %z`)

If columns is not set, this plugin detect schema automatically by using avsc schema.

## Example

```yaml
in:
  type: file
  path_prefix: "items"
  parser:
    type: avro
    columns:
      - {name: "id", type: "long"}
      - {name: "code", type: "string"}
      - {name: "name", type: "string"}
      - {name: "description", type: "string"}
      - {name: "flag", type: "boolean"}
      - {name: "price", type: "long"}
      - {name: "item_type", type: "string"}
      - {name: "tags", type: "json"}
      - {name: "options", type: "json"}
      - {name: "spec", type: "json"}
      - {name: "created_at", type: "timestamp", format: "%Y-%m-%dT%H:%M:%S%:z"}
      - {name: "created_at_utc", type: "timestamp"}

out:
  type: stdout
```

```javascript
// item.avsc

{
  "type" : "record",
  "name" : "Item",
  "namespace" : "example.avro",
  "fields" : [
    {"name": "id", "type": "int"},
    {"name": "code", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "description", "type": ["string", "null"]},
    {"name": "flag", "type": "boolean"},
    {"name": "created_at", "type": "string"},
    {"name": "created_at_utc", "type": "float"},
    {"name": "price", "type": ["double", "null"]},
    {"name": "spec", "type": {
      "type": "record",
      "name": "item_spec",
      "fields" : [
        {"name" : "key", "type" : "string"},
        {"name" : "value", "type" : ["string", "null"]}
      ]}
    },
    {"name": "tags", "type": [{"type": "array", "items": "string"}, "null"]},
    {"name": "options", "type": {"type": "map", "values": ["string", "null"]}},
    {"name": "item_type", "type": {"name": "item_type_enum", "type": "enum", "symbols": ["D", "M"]}},
    {"name": "dummy", "type": "null"}
  ]
}
```

(If guess supported) you don't have to write `parser:` section in the configuration file. After writing `in:` section, you can let embulk guess `parser:` section using this command:

```
$ embulk gem install embulk-parser-avro
$ embulk guess -g avro config.yml -o guessed.yml
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
