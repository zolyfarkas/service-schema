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
import org.apache.avro.util.Optional;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonExtensionDecoder;
import org.apache.avro.io.JsonExtensionEncoder;

/**
 * Decimal represents arbitrary-precision fixed-scale decimal numbers
 */
public final class JsonLogicalTypeBytes<T> extends AbstractLogicalType<T> {

  JsonLogicalTypeBytes(final Schema.Type type, final String logicalTypeName, final Class<T> clasz) {
    super(type, Collections.EMPTY_SET, logicalTypeName, Collections.EMPTY_MAP , clasz);
    if (type != Schema.Type.BYTES) {
       throw new IllegalArgumentException(this.logicalTypeName + " must be backed by string or bytes, not" + type);
    }
  }


  @Override
  public T deserialize(Object object) {
    byte[] unscaled;
    if (object instanceof ByteBuffer) {
      unscaled = readByteBuffer((ByteBuffer) object);
    } else {
      unscaled = (byte[]) object;
    }
    try {
      return Schema.MAPPER.readValue(new ByteArrayInputStream(unscaled), getLogicalJavaType());
    } catch (IOException ex) {
      throw new UncheckedIOException("Cannot deserialize " + object, ex);
    }

  }

  static byte[] readByteBuffer(final ByteBuffer buf) {
    buf.rewind();
    byte[] unscaled = new byte[buf.remaining()];
    buf.get(unscaled);
    return unscaled;
  }

  @Override
  public Object serialize(T json) {
    ByteArrayOutputStream bab = new ByteArrayOutputStream();
    try {
      Schema.MAPPER.writeValue(bab, json);
    } catch (IOException ex) {
      throw new UncheckedIOException("Cannot serialize " + json, ex);
    }
    return ByteBuffer.wrap(bab.toByteArray());
  }

  @Override
  public Optional<T> tryDirectDecode(Decoder dec, final Schema schema) throws IOException {
    if (dec instanceof JsonExtensionDecoder) {
      JsonExtensionDecoder pd = (JsonExtensionDecoder) dec;
      return Optional.of(pd.readValue(schema, getLogicalJavaType()));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public boolean tryDirectEncode(T object, Encoder enc, final Schema schema) throws IOException {
    if (enc instanceof JsonExtensionEncoder) {
      ((JsonExtensionEncoder) enc).writeValue(object, schema);
      return true;
    } else {
      return false;
    }
  }
}
