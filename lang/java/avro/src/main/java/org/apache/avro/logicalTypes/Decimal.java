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
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
import org.codehaus.jackson.JsonNode;

  /** Decimal represents arbitrary-precision fixed-scale decimal numbers  */
public final class Decimal extends AbstractLogicalType {

    private static final Set<String> RESERVED = AbstractLogicalType.reservedSet("precision", "scale",
            "serRounding", "deserRounding");

    private final MathContext mc;
    private final int scale;
    private final int precision;
    private final RoundingMode serRm;
    private final RoundingMode deserRm;

    public Decimal(Integer precision, Integer scale, Schema.Type type, RoundingMode serRm, RoundingMode deserRm) {
      super(type, RESERVED, "decimal", toAttributes(precision, scale, serRm, deserRm));
      this.precision = precision;
      this.serRm = serRm;
      this.deserRm = deserRm;
      if (precision <= 0) {
        throw new IllegalArgumentException("Invalid " + this.logicalTypeName + " precision: " +
            precision + " (must be positive)");
      }
      if (scale < 0) {
        throw new IllegalArgumentException("Invalid " + this.logicalTypeName + " scale: " +
            scale + " (must be positive)");
      } else if (scale > precision) {
        throw new IllegalArgumentException("Invalid " + this.logicalTypeName + " scale: " +
            scale + " (greater than precision: " + precision + ")");
      }
      mc = new MathContext(precision, RoundingMode.HALF_EVEN);
      this.scale = scale;
    }

    public Decimal(JsonNode node, Schema.Type type) {
        this(getInteger(node, "precision"), getInteger(node, "scale"), type,
                getRoundingMode(node, "serRounding"), getRoundingMode(node, "deserRounding"));
    }

    private static Map<String, Object> toAttributes(Integer precision, Integer scale,
            RoundingMode serRm, RoundingMode deserRm) {
       Map<String, Object> attr = new HashMap<String, Object>(5);
       attr.put("precision", precision == null ? Integer.valueOf(36) : precision);
       attr.put("scale", scale == null ?  (precision == null ? Integer.valueOf(12) : precision / 2) : scale);
       if (serRm != null) {
         attr.put("serRounding", serRm.toString());
       }
       if (deserRm != null) {
         attr.put("deserRounding", deserRm.toString());
       }
       return attr;
    }

    private static Integer getInteger(JsonNode node, String fieldName) {
      JsonNode n = node.get(fieldName);
      if (n == null) {
        return null;
      } else {
        return n.asInt();
      }
    }

    private static RoundingMode getRoundingMode(JsonNode node, String fieldName) {
      JsonNode n = node.get(fieldName);
      if (n == null) {
        return null;
      } else {
        return RoundingMode.valueOf(n.getTextValue());
      }
    }


    @Override
    public void validate(Schema schema) {
      Schema.Type type1 = schema.getType();
      // validate the type
      if (type1 != Schema.Type.BYTES &&
          type1 != Schema.Type.STRING) {
        throw new IllegalArgumentException(this.logicalTypeName + " must be backed by fixed or bytes, not" + type1);
      }
      int precision = mc.getPrecision();
      if (precision > maxPrecision(schema)) {
        throw new IllegalArgumentException("Invalid precision " + precision);
      }
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
      return logicalType.getClass() == Decimal.class;
    }


    @Override
    public Set<String> reserved() {
      return RESERVED;
    }

    private long maxPrecision(Schema schema) {
      if (schema.getType() == Schema.Type.BYTES
              || schema.getType() == Schema.Type.STRING) {
        // not bounded
        return Integer.MAX_VALUE;
      } else {
        // not valid for any other type
        return 0;
      }
    }

    @Override
    public Class<?> getLogicalJavaType() {
        return BigDecimal.class;
    }

    @Override
    public Object deserialize(Object object) {
      switch (type) {
        case STRING:
          BigDecimal result = new BigDecimal(object.toString(), mc);
          if (result.scale() > scale) {
            if (deserRm != null) {
              result = result.setScale(scale, deserRm);
            } else {
              throw new AvroRuntimeException("Received Decimal " + object + " is not compatible with scale " + scale);
            }
          }
          return result;
        case BYTES:
          //ByteBuffer buf = ByteBuffer.wrap((byte []) object);
          ByteBuffer buf = (ByteBuffer) object;
          buf.rewind();
          int lscale = readInt(buf);
          if (lscale > scale && deserRm != null) {
              throw new AvroRuntimeException("Received Decimal " + object + " is not compatible with scale " + scale);
          }
          byte[] unscaled = new byte[buf.remaining()];
          buf.get(unscaled);
          BigInteger unscaledBi = new BigInteger(unscaled);
          BigDecimal r = new BigDecimal(unscaledBi, lscale);
          if (lscale > scale && deserRm != null) {
            r = r.setScale(scale, deserRm);
          }
          return r;
        default:
          throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
      }

    }

    @Override
    public Object serialize(Object object) {
      BigDecimal decimal = (BigDecimal) object;
      if (decimal.scale() > scale) {
        if (serRm != null) {
          decimal = decimal.setScale(scale, serRm);
        } else {
          throw new UnsupportedOperationException("Decimal " + decimal + " exceeds scale " + scale);
        }
      }
      if (decimal.precision() > precision) {
        throw new UnsupportedOperationException("Decimal " + decimal + " exceeds precision " + precision);
      }
      switch (type) {
        case STRING:
          return decimal.toPlainString();
        case BYTES:
          return toBytes(decimal);
        default:
          throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
      }
    }

  public static ByteBuffer toBytes(BigDecimal decimal) {
    byte[] unscaledValue = decimal.unscaledValue().toByteArray();
    ByteBuffer buf = ByteBuffer.allocate(5 + unscaledValue.length);
    writeInt(decimal.scale(), buf);
    buf.put(unscaledValue);
    buf.flip();
    return buf;
  }

  public static int readInt(final ByteBuffer buf) {
    int n = 0;
    int b;
    int shift = 0;
    do {
      b = buf.get() & 0xff;
      n |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return (n >>> 1) ^ -(n & 1); // back to two's-complement
      }
      shift += 7;
    } while (shift < 32);
    throw new RuntimeException("Invalid int encoding" + buf);
  }

  public static void writeInt(final int n, final ByteBuffer buf) {
    int val = (n << 1) ^ (n >> 31);
    if ((val & ~0x7F) == 0) {
      buf.put((byte) val);
      return;
    } else if ((val & ~0x3FFF) == 0) {
      buf.put((byte) (0x80 | val));
      buf.put((byte) (val >>> 7));
      return;
    }
    byte [] tmp = new byte[5];
    int len = BinaryData.encodeInt(n, tmp, 0);
    buf.put(tmp, 0, len);
  }


}
