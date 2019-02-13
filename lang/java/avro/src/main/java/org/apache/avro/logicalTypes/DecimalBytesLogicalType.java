/*
 * Copyright 2019 The Apache Software Foundation.
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
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;

/**
 *
 * @author Zoltan Farkas
 */
public final class DecimalBytesLogicalType extends DecimalBase {

  public DecimalBytesLogicalType(Number precision, Number scale, Schema.Type type,
          RoundingMode serRm, RoundingMode deserRm) {
    super(precision, scale, type, serRm, deserRm);
  }

  @Override
  public BigDecimal doDeserialize(Object object) {
    ByteBuffer buf;
    if (object instanceof byte[]) {
      buf = ByteBuffer.wrap((byte[]) object);
    } else {
      buf = (ByteBuffer) object;
      buf.rewind();
    }
    int lscale = readInt(buf);
    byte[] unscaled = new byte[buf.remaining()];
    buf.get(unscaled);
    BigInteger unscaledBi = new BigInteger(unscaled);
    return new BigDecimal(unscaledBi, lscale);
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

  public static ByteBuffer toBytes(BigDecimal decimal) {
    byte[] unscaledValue = decimal.unscaledValue().toByteArray();
    ByteBuffer buf = ByteBuffer.allocate(5 + unscaledValue.length);
    writeInt(decimal.scale(), buf);
    buf.put(unscaledValue);
    buf.flip();
    return buf;
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
    byte[] tmp = new byte[5];
    int len = BinaryData.encodeInt(n, tmp, 0);
    buf.put(tmp, 0, len);
  }


  @Override
  public Object doSerialize(final BigDecimal decimal) {
    return toBytes(decimal);
  }


}
