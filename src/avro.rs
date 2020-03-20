use std::collections::HashMap;

use avro_rs::schema::Schema;
use avro_rs::types::Value as AvroValue;
use avro_rs::types::Record;
use byteorder::{NetworkEndian, WriteBytesExt};

use crate::error::Result;

pub static SCHEMA: &str = r#"
{
  "type": "record",
  "name": "Envelope",
  "namespace": "onenightultimatewerewolf.record",
  "fields": [
    {
      "name": "before",
      "type": [
        "null",
        {
          "type": "record",
          "name": "Value",
          "fields": [
            {
              "name": "player",
              "type": "string"
            },
            {
              "name": "role",
              "type": "string"
            }
        ],
        "default": null
      }
     ]
    },
    {
       "name": "after",
       "type": [
          "null",
          "Value"
       ],
       "default": null
    }
  ]
}
"#;

fn parse_schema(schema: &str) -> Result<Schema> {
    // munge resolves named types in Avro schemas, which are not currently
    // supported by our Avro library. Follow [0] for details.
    //
    // [0]: https://github.com/flavray/avro-rs/pull/53
    //
    // TODO(benesch): fix this upstream.
    fn munge(
        schema: serde_json::Value,
        types: &mut HashMap<String, serde_json::Value>,
    ) -> serde_json::Value {
        use serde_json::Value::*;
        match schema {
            Null | Bool(_) | Number(_) => schema,

            String(s) => match s.as_ref() {
                "null" | "boolean" | "int" | "long" | "float" | "double" | "bytes" | "string" => {
                    String(s)
                }
                other => types.get(other).cloned().unwrap_or_else(|| String(s)),
            },

            Array(vs) => Array(vs.into_iter().map(|v| munge(v, types)).collect()),

            Object(mut map) => {
                if let Some(String(name)) = map.get("name") {
                    types.insert(name.clone(), Object(map.clone()));
                }
                if let Some(fields) = map.remove("fields") {
                    let fields = match fields {
                        Array(fields) => Array(
                            fields
                                .into_iter()
                                .map(|f| match f {
                                    Object(mut fmap) => {
                                        if let Some(typ) = fmap.remove("type") {
                                            fmap.insert("type".into(), munge(typ, types));
                                        }
                                        Object(fmap)
                                    }
                                    other => other,
                                })
                                .collect(),
                        ),
                        other => other,
                    };
                    map.insert("fields".into(), fields);
                }
                Object(map)
            }
        }
    }
    let schema = serde_json::from_str(schema)?;
    let schema = munge(schema, &mut HashMap::new());
    let ret = Schema::parse(&schema).expect("Avro parsing failed");
    Ok(ret)
}

fn encode_record(schema: &Schema, record: AvroValue) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.write_u8(0).unwrap();
    buf.write_i32::<NetworkEndian>(0).unwrap();
    buf.extend(avro_rs::to_avro_datum(&schema, record).unwrap());
    buf
}

pub fn encode_insert(player: &str, role: &str) -> Result<Vec<u8>> {
    let schema = parse_schema(SCHEMA)?;
    let record = AvroValue::Record(vec![
        ("before".into(), AvroValue::Union(Box::new(AvroValue::Null))),
        (
            "after".into(),
            AvroValue::Union(Box::new(AvroValue::Record(vec![
                ("player".into(), AvroValue::String(player.into())),
                ("role".into(), AvroValue::String(role.into())),
            ]))),
        ),
    ]);

    Ok(encode_record(&schema, record))
}

pub fn encode_delete(player: &str, role: &str) -> Result<Vec<u8>> {
    let schema = parse_schema(SCHEMA)?;
    let record = AvroValue::Record(vec![
        (
            "before".into(),
            AvroValue::Union(Box::new(AvroValue::Record(vec![
                ("player".into(), AvroValue::String(player.into())),
                ("role".into(), AvroValue::String(role.into())),
            ]))),
        ),
      ("after".into(), AvroValue::Union(Box::new(AvroValue::Null))),
    ]);

    Ok(encode_record(&schema, record))
}
