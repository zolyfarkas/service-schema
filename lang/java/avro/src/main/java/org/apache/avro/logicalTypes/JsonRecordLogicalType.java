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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonExtensionDecoder;
import org.apache.avro.io.JsonExtensionEncoder;

/**
 * Decimal represents arbitrary-precision fixed-scale decimal numbers
 */
public final class JsonRecordLogicalType extends AbstractLogicalType<Map> {

  JsonRecordLogicalType(final Schema.Type type) {
    super(type, Collections.EMPTY_SET, "json_record", Collections.EMPTY_MAP , Map.class);
    if (type != Schema.Type.BYTES) {
       throw new IllegalArgumentException(this.logicalTypeName + " must be backed by string or bytes, not" + type);
    }
  }


  @Override
  public Map deserialize(Object object) {
    switch (type) {
      case BYTES:
        ByteBuffer buf = (ByteBuffer) object;
        buf.rewind();
        byte[] unscaled = new byte[buf.remaining()];
        buf.get(unscaled);
        try {
          return Schema.MAPPER.readValue(new ByteArrayInputStream(unscaled), Map.class);
        } catch (IOException ex) {
          throw new UncheckedIOException(ex);
        }
      default:
        throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
    }

  }

  @Override
  public Object serialize(Map json) {
    switch (type) {
      case BYTES:
        ByteArrayOutputStream bab = new ByteArrayOutputStream();
        try {
          Schema.MAPPER.writeValue(bab, json);
        } catch (IOException ex) {
          throw new UncheckedIOException(ex);
        }
        return ByteBuffer.wrap(bab.toByteArray());
      default:
        throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
    }
  }

  @Override
  public Map tryDirectDecode(Decoder dec, final Schema schema) throws IOException {
    if (dec instanceof JsonExtensionDecoder) {
      JsonExtensionDecoder pd = (JsonExtensionDecoder) dec;
      return (pd).readValue(schema, Map.class);
    } else {
      return null;
    }
  }

  @Override
  public boolean tryDirectEncode(Map object, Encoder enc, final Schema schema) throws IOException {
    if (enc instanceof JsonExtensionEncoder) {
      ((JsonExtensionEncoder) enc).writeValue(object, schema);
      return true;
    } else {
      return false;
    }
  }



}
