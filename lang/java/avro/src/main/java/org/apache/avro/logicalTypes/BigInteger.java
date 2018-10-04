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
import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.io.DecimalDecoder;
import org.apache.avro.io.DecimalEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

/**
 * Decimal represents arbitrary-precision fixed-scale decimal numbers
 */
public final class BigInteger extends AbstractLogicalType<java.math.BigInteger> {

  BigInteger(Schema.Type type) {
    super(type, Collections.EMPTY_SET, "bigint", Collections.EMPTY_MAP, java.math.BigInteger.class);
    if (type != Schema.Type.BYTES
            && type != Schema.Type.STRING) {
      throw new IllegalArgumentException(this.logicalTypeName + " must be backed by string or bytes, not" + type);
    }
  }

  @Override
  public java.math.BigInteger deserialize(Object object) {
    switch (type) {
      case STRING:
        return new java.math.BigInteger(object.toString());
      case BYTES:
        //ByteBuffer buf = ByteBuffer.wrap((byte []) object);
        ByteBuffer buf = (ByteBuffer) object;
        buf.rewind();
        int scale = Decimal.readInt(buf);
        if (scale != 0) {
          throw new RuntimeException("Scale must be zero and not " + scale);
        }
        byte[] unscaled = new byte[buf.remaining()];
        buf.get(unscaled);
        return new java.math.BigInteger(unscaled);
      default:
        throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
    }

  }

  @Override
  public Object serialize(java.math.BigInteger object) {
    switch (type) {
      case STRING:
        return object.toString();
      case BYTES:
        return toBytes(object);
      default:
        throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
    }
  }

  @Override
  public boolean tryDirectEncode(final java.math.BigInteger object, final Encoder enc, final Schema schema)
          throws IOException {
    if (enc instanceof DecimalEncoder) {
      ((DecimalEncoder) enc).writeBigInteger(object, schema);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public java.math.BigInteger tryDirectDecode(final Decoder dec, final Schema schema) throws IOException {
    if (dec instanceof DecimalDecoder) {
      return ((DecimalDecoder) dec).readBigInteger(schema);
    } else {
      return null;
    }
  }


  public static ByteBuffer toBytes(java.math.BigInteger integer) {
    byte[] unscaledValue = integer.toByteArray();
    ByteBuffer buf = ByteBuffer.allocate(1 + unscaledValue.length);
    buf.put((byte) 0);
    buf.put(unscaledValue);
    buf.rewind();
    return buf;
  }

  public static boolean is(final Schema schema) {
    Schema.Type type1 = schema.getType();
    // validate the type
    if (type1 != Schema.Type.BYTES && type1 != Schema.Type.STRING) {
      return false;
    }
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType == null) {
      return false;
    }
    return logicalType.getClass() == BigInteger.class;
  }
}
