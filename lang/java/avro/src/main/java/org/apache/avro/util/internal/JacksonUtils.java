/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.util.internal;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.TokenBuffer;

public class JacksonUtils {
  public static final String BYTES_CHARSET = "ISO-8859-1";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JacksonUtils() {
  }

  public static JsonNode toJsonNode(Object datum) {
    if (datum == null) {
      return null;
    }
    try {
      TokenBuffer generator = new TokenBuffer(MAPPER);
      toJson(datum, generator);
      return MAPPER.readTree(generator.asParser());
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  @SuppressWarnings(value="unchecked")
  static void toJson(Object datum, JsonGenerator generator) throws IOException {
    if (datum == JsonProperties.NULL_VALUE) { // null
      generator.writeNull();
    } else if (datum instanceof Map) { // record, map
      generator.writeStartObject();
      for (Map.Entry<Object,Object> entry : ((Map<Object,Object>) datum).entrySet()) {
        generator.writeFieldName(entry.getKey().toString());
        toJson(entry.getValue(), generator);
      }
      generator.writeEndObject();
    } else if (datum instanceof Collection) { // array
      generator.writeStartArray();
      for (Object element : (Collection<?>) datum) {
        toJson(element, generator);
      }
      generator.writeEndArray();
    } else if (datum instanceof byte[]) { // bytes, fixed
      generator.writeString(new String((byte[]) datum, BYTES_CHARSET));
    }  else if (datum instanceof GenericFixed) { // bytes, fixed
      generator.writeString(new String(((GenericFixed) datum).bytes(), BYTES_CHARSET));
    } else if (datum instanceof ByteBuffer) {
        ByteBuffer bytes = ((ByteBuffer) datum);
        byte[] data = copyOfBytes(bytes);
       generator.writeString(new String(data, BYTES_CHARSET));
    } else if (datum instanceof CharSequence || datum instanceof Enum<?>) { // string, enum
      generator.writeString(datum.toString());
    } else if (datum instanceof Double) { // double
      generator.writeNumber((Double) datum);
    } else if (datum instanceof Float) { // float
      generator.writeNumber((Float) datum);
    } else if (datum instanceof Long) { // long
      generator.writeNumber((Long) datum);
    } else if (datum instanceof Integer) { // int
      generator.writeNumber((Integer) datum);
    } else if (datum instanceof Boolean) { // boolean
      generator.writeBoolean((Boolean) datum);
    } else if (datum instanceof GenericEnumSymbol) {
      generator.writeString(datum.toString());
    } else if (datum instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) datum;
      try {
        Schema schema = record.getSchema();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, generator);
        DatumWriter writer = new GenericDatumWriter(schema);
        writer.write(record, encoder);
        encoder.flush();
      } catch (IOException e) {
        throw new AvroRuntimeException(e);
      }
      JsonNode tree = MAPPER.readTree(GenericData.get().toString(datum));
      generator.writeTree(tree);
    } else {
      throw new AvroRuntimeException("Unknown datum class: " + datum.getClass());
    }
  }

  public static byte[] copyOfBytes(final ByteBuffer bytes) {
    bytes.mark();
    byte[] data = new byte[bytes.remaining()];
    bytes.get(data);
    bytes.reset(); // put the buffer back the way we got it
    return data;
  }

  public static Object toObject(JsonNode jsonNode) {
    return toObject(jsonNode, null);
  }

  public static Object toObject(JsonNode jsonNode, Schema schema) {
    if (schema != null && schema.getType().equals(Schema.Type.UNION)) {
      return toObject(jsonNode, schema.getTypes().get(0));
    }
    if (jsonNode == null) {
      return null;
    } else if (jsonNode.isNull()) {
      return JsonProperties.NULL_VALUE;
    } else if (jsonNode.isBoolean()) {
      return jsonNode.asBoolean();
    } else if (jsonNode.isInt()) {
      if (schema == null || schema.getType().equals(Schema.Type.INT)) {
        return jsonNode.asInt();
      } else if (schema.getType().equals(Schema.Type.LONG)) {
        return jsonNode.asLong();
      }
    } else if (jsonNode.isLong()) {
      return jsonNode.asLong();
    } else if (jsonNode.isDouble()) {
      if (schema == null || schema.getType().equals(Schema.Type.DOUBLE)) {
        return jsonNode.asDouble();
      } else if (schema.getType().equals(Schema.Type.FLOAT)) {
        return (float) jsonNode.asDouble();
      }
    } else if (jsonNode.isTextual()) {
      if (schema == null || schema.getType().equals(Schema.Type.STRING) ||
          schema.getType().equals(Schema.Type.ENUM)) {
        return jsonNode.asText();
      } else if (schema.getType().equals(Schema.Type.BYTES)) {
        try {
          return jsonNode.getTextValue().getBytes(BYTES_CHARSET);
        } catch (UnsupportedEncodingException e) {
          throw new AvroRuntimeException(e);
        }
      } else if (schema.getType().equals(Schema.Type.FIXED)) {
        try {
          return new Fixed(schema, jsonNode.getTextValue().getBytes(BYTES_CHARSET));
        } catch (UnsupportedEncodingException e) {
          throw new AvroRuntimeException(e);
        }
      }
    } else if (jsonNode.isArray()) {
      List l = new ArrayList();
      for (JsonNode node : jsonNode) {
        l.add(toObject(node, schema == null ? null : schema.getElementType()));
      }
      return l;
    } else if (jsonNode.isObject()) {
      Map m = new LinkedHashMap();
      for (Iterator<String> it = jsonNode.getFieldNames(); it.hasNext(); ) {
        String key = it.next();
        Schema s = null;
        if (schema == null) {
          s = null;
        } else if (schema.getType().equals(Schema.Type.MAP)) {
          s = schema.getValueType();
        } else if (schema.getType().equals(Schema.Type.RECORD)) {
          s = schema.getField(key).schema();
        }
        Object value = toObject(jsonNode.get(key), s);
        m.put(key, value);
      }
      return m;
    }
    return null;
  }
}
