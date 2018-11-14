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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;

/**
 * Decimal represents arbitrary-precision fixed-scale decimal numbers
 */
public final class AvroDecimal extends AbstractLogicalType<BigDecimal> {


  private static final Set<String> RESERVED = AbstractLogicalType.reservedSet("precision", "scale");

  private final int scale;

  private final Schema schema;

  AvroDecimal(Number scale, Schema schema) {
    super(schema.getType(), RESERVED, "decimal", toAttributes(scale), BigDecimal.class);
    this.schema = schema;
    if (type != Schema.Type.BYTES && type != Schema.Type.FIXED) {
       throw new IllegalArgumentException(this.logicalTypeName + " must be backed by string or bytes, not" + type);
    }
    if (scale == null) {
      scale = 0;
    } else if (scale.intValue() < 0) {
      throw new IllegalArgumentException("Invalid " + this.logicalTypeName + " scale: "
              + scale + " (must be positive)");
    }
    this.scale = scale.intValue();
  }

  private static Map<String, Object> toAttributes(Number scale) {
    Map<String, Object> attr = new HashMap<String, Object>(2);
    if (scale != null) {
      attr.put("scale", scale);
    }
    return attr;
  }

  @Override
  public BigDecimal deserialize(Object object) {
    switch (type) {
      case FIXED:
        return new BigDecimal(new BigInteger(((GenericFixed) object).bytes()), scale);
      case BYTES:
        ByteBuffer value;
        if (object instanceof byte[]) {
          return new BigDecimal(new BigInteger((byte[]) object), scale);
        } else {
          value = (ByteBuffer) object;
        }
        byte[] bytes = new byte[value.remaining()];
        value.get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
      default:
        throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
    }

  }

  @Override
  public Object serialize(BigDecimal decimal) {
    switch (type) {
      case FIXED:
              if (scale != decimal.scale()) {
        throw new AvroTypeException("Cannot encode decimal with scale " +
            decimal.scale() + " as scale " + scale);
      }

      byte fillByte = (byte) (decimal.signum() < 0 ? 0xFF : 0x00);
      byte[] unscaled = decimal.unscaledValue().toByteArray();
      byte[] bytes = new byte[schema.getFixedSize()];
      int offset = bytes.length - unscaled.length;

      for (int i = 0; i < bytes.length; i += 1) {
        if (i < offset) {
          bytes[i] = fillByte;
        } else {
          bytes[i] = unscaled[i - offset];
        }
      }
      return new GenericData.Fixed(schema, bytes);

      case BYTES:
      if (scale != decimal.scale()) {
        throw new AvroTypeException("Cannot encode decimal with scale " +
            decimal.scale() + " as scale " + scale);
      }
      return ByteBuffer.wrap(decimal.unscaledValue().toByteArray());
      default:
        throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
    }
  }


}
