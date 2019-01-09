/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.logicalTypes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.AvroNamesRefResolver;
import org.apache.avro.Schema;
import org.apache.avro.SchemaResolver;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.ExtendedJsonDecoder;
import org.apache.avro.io.JsonExtensionDecoder;
import org.apache.avro.io.JsonExtensionEncoder;
import org.apache.avro.reflect.ExtendedReflectData;
import org.apache.avro.reflect.ExtendedReflectDatumWriter;
import org.apache.avro.util.CharSequenceReader;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;

/**
 * Decimal represents arbitrary-precision fixed-scale decimal numbers
 */
public final class AnyAvroLogicalType extends AbstractLogicalType<Object> {


  private final SchemaResolver resolver;

  private final Schema uSchema;

  private final int schemaIdx;

  private final int contentIdx;

  AnyAvroLogicalType(Schema schema, final SchemaResolver resolver) {
    super(schema.getType(), Collections.EMPTY_SET, "any",
            Collections.EMPTY_MAP, Object.class);
    if (type != Schema.Type.RECORD) {
       throw new IllegalArgumentException(this.logicalTypeName + " must be backed by string, not" + type);
    }
    Schema.Field sField = schema.getField("avsc");
    Schema.Field cField = schema.getField("content");
    if (sField == null || cField == null) {
      throw new IllegalArgumentException("Schema " + schema + " must have fields 'avsc' and 'content'");
    }
    if (sField.schema().getType() != Schema.Type.STRING) {
      throw new IllegalArgumentException("Schema " + schema + " field 'avsc' must have string type");
    }
    if (cField.schema().getType() != Schema.Type.BYTES) {
      throw new IllegalArgumentException("Schema " + schema + " field 'content' must have bytes type");
    }
    contentIdx = cField.pos();
    schemaIdx = sField.pos();
    this.uSchema = schema;
    this.resolver = resolver;
  }

  @Override
  public Object deserialize(Object object) {
    GenericRecord rec = (GenericRecord) object;
    CharSequence schema = (CharSequence) rec.get(schemaIdx);
    Schema sch;
    try {
      sch = new Schema.Parser(new AvroNamesRefResolver(resolver)).parse(
              Schema.FACTORY.createJsonParser(new CharSequenceReader(schema)));
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
    ByteBuffer bb = (ByteBuffer) rec.get(contentIdx);
    DatumReader reader = new GenericDatumReader(sch, sch);
    int arrayOffset = bb.arrayOffset();
    InputStream is = new ByteArrayInputStream(bb.array(), arrayOffset, bb.limit() - arrayOffset);
    try {
      Decoder decoder = DecoderFactory.get().binaryDecoder(is, null);
      return reader.read(null, decoder);
    } catch (IOException | RuntimeException ex) {
      throw new RuntimeException(this + " parsing failed for " + sch + ", from " + is, ex);
    }
  }

  @Override
  public Object serialize(Object toSer) {
      Schema schema = ExtendedReflectData.get().getSchema(toSer.getClass());
      if (schema == null) {
        schema = ExtendedReflectData.get().createSchema(toSer.getClass(), toSer, new HashMap<>());
      }
      StringWriter sw = new StringWriter();
      try {
        JsonGenerator jgen = Schema.FACTORY.createJsonGenerator(sw);
        schema.toJson(new AvroNamesRefResolver(resolver), jgen);
        jgen.flush();
       } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
      String strSchema = sw.toString();
      GenericRecord result = new GenericData.Record(uSchema);
      result.put(schemaIdx, strSchema);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DatumWriter writer = new ExtendedReflectDatumWriter(schema);
      Encoder encoder = EncoderFactory.get().binaryEncoder(bos, null);
      try {
        writer.write(toSer, encoder);
        encoder.flush();
      } catch (IOException | RuntimeException ex) {
        throw new RuntimeException("Cannot serialize " + toSer, ex);
      }
      result.put(contentIdx, ByteBuffer.wrap(bos.toByteArray()));
      return result;
    }

  @Override
  public Object tryDirectDecode(Decoder dec, final Schema schema) throws IOException {
    if (dec instanceof JsonExtensionDecoder) {
      JsonExtensionDecoder pd = (JsonExtensionDecoder) dec;
      JsonNode theJson = pd.readValueAsTree(schema);
      JsonNode get = theJson.get("avsc");
      Schema anySchema = Schema.parse(get, new AvroNamesRefResolver(resolver), true);
      JsonNode cntnt = theJson.get("content");
      String jsonString = Schema.MAPPER.writeValueAsString(cntnt);
      ExtendedJsonDecoder jdec = new ExtendedJsonDecoder(anySchema, jsonString);
      DatumReader reader = new GenericDatumReader(anySchema, anySchema);
      return reader.read(null, jdec);
    } else {
      return null;
    }
  }

  @Override
  public boolean tryDirectEncode(Object toSer, Encoder enc, final Schema schema) throws IOException {
    if (enc instanceof JsonExtensionEncoder) {
      Schema avsc = ExtendedReflectData.get().getSchema(toSer.getClass());
      if (avsc == null) {
        avsc = ExtendedReflectData.get().createSchema(toSer.getClass(), toSer, new HashMap<>());
      }
      Map record = new HashMap(4);
      record.put("avsc", avsc);
      record.put("content", toSer);
      ((JsonExtensionEncoder) enc).writeValue(record, schema);
      return true;
    } else {
      return false;
    }
  }



}
