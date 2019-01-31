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

import java.io.IOException;
import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonExtensionDecoder;
import org.apache.avro.io.JsonExtensionEncoder;

/**
 * Decimal represents arbitrary-precision fixed-scale decimal numbers
 */
public final class BigIntegerString extends AbstractLogicalType<java.math.BigInteger> {

  @Nullable private final Integer precision;

  BigIntegerString(Schema.Type type, @Nullable Integer precision) {
    super(type, Collections.EMPTY_SET, "bigint", BigIntegerFactory.toAttributes(precision), java.math.BigInteger.class);
    if (type != Schema.Type.STRING) {
      throw new IllegalArgumentException(this.logicalTypeName + " must be backed by string or bytes, not" + type);
    }
    this.precision = precision;
  }

  @Override
  public java.math.BigInteger deserialize(Object object) {
      return new java.math.BigInteger(object.toString());
  }

  @Override
  public Object serialize(java.math.BigInteger object) {
    String res = object.toString();
    if (precision != null) {
      if (res.length() > precision) {
        throw new AvroRuntimeException("unable to serialize due to precision limitation " + precision + ", nr " + object);
      }
    }
    return res;
  }

  @Override
  public boolean tryDirectEncode(final java.math.BigInteger object, final Encoder enc, final Schema schema)
          throws IOException {
    if (enc instanceof JsonExtensionEncoder) {
      ((JsonExtensionEncoder) enc).writeBigInteger(object, schema);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public java.math.BigInteger tryDirectDecode(final Decoder dec, final Schema schema) throws IOException {
    if (dec instanceof JsonExtensionDecoder) {
      return ((JsonExtensionDecoder) dec).readBigInteger(schema);
    } else {
      return null;
    }
  }

}
