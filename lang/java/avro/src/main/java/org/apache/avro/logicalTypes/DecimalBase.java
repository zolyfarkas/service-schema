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
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.DecimalEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonExtensionDecoder;
import org.apache.avro.io.JsonExtensionEncoder;

/**
 * Decimal represents precision and scale limited decimal numbers.
 */
public abstract class DecimalBase extends AbstractLogicalType<BigDecimal> {

  private static final RoundingMode DEFAULT_DESER_ROUNDING = getRoundingMode("avro.decimal.defaultDeserRounding");
  private static final RoundingMode DEFAULT_SER_ROUNDING = getRoundingMode("avro.decimal.defaultSerRounding");

  private static RoundingMode getRoundingMode(final String property) {
    String sdr = System.getProperty(property, "none");
    if (sdr == null || sdr.isEmpty() || "none".equalsIgnoreCase(sdr)) {
      return null;
    } else {
      return RoundingMode.valueOf(sdr);
    }
  }

  private static final Set<String> RESERVED = AbstractLogicalType.reservedSet("precision", "scale",
          "serRounding", "deserRounding");

  final MathContext mc;
  private final int scale;
  private final int precision;
  private final RoundingMode serRm;
  private final RoundingMode deserRm;

  DecimalBase(Number precision, Number scale, Schema.Type type, RoundingMode serRm, RoundingMode deserRm) {
    super(type, RESERVED, "decimal", toAttributes(precision, scale, serRm, deserRm), BigDecimal.class);
    precision = precision == null ? 36 : precision;
    this.serRm = serRm == null ? DEFAULT_SER_ROUNDING : serRm;
    this.deserRm = deserRm == null ? DEFAULT_DESER_ROUNDING : deserRm;
    if (precision.intValue() <= 0) {
      throw new IllegalArgumentException("Invalid " + this.logicalTypeName + " precision: "
              + precision + " (must be positive)");
    }
    scale = scale == null ? (precision == null ? 12 : precision.intValue() / 2) : scale;
    int sInt = scale.intValue();
    int pInt = precision.intValue();
    if (sInt < 0) {
      throw new IllegalArgumentException("Invalid " + this.logicalTypeName + " scale: "
              + scale + " (must be positive)");
    } else if (sInt > pInt) {
      throw new IllegalArgumentException("Invalid " + this.logicalTypeName + " scale: "
              + scale + " (greater than precision: " + precision + ")");
    }
    mc = new MathContext(pInt, RoundingMode.HALF_EVEN);
    this.scale = sInt;
    this.precision = pInt;
  }

  private static Map<String, Object> toAttributes(Number precision, Number scale,
          RoundingMode serRm, RoundingMode deserRm) {
    Map<String, Object> attr = new HashMap<String, Object>(4);
    if (precision != null) {
      attr.put("precision", precision);
    }
    if (scale != null) {
      attr.put("scale", scale);
    }
    if (serRm != null) {
      attr.put("serRounding", serRm.toString());
    }
    if (deserRm != null) {
      attr.put("deserRounding", deserRm.toString());
    }
    return attr;
  }


  public abstract BigDecimal doDeserialize(Object object);

  @Override
  public BigDecimal deserialize(Object object) {
    if (BigDecimal.class == object.getClass()) {
      return (BigDecimal) object;
    }
    BigDecimal result = doDeserialize(object);
    if (result.scale() > scale) {
      if (deserRm != null) {
        result = result.setScale(scale, deserRm);
      } else {
        throw new AvroRuntimeException("Received Decimal " + object + " is not compatible with scale " + scale
                + " if you desire rounding, you can annotate type with @deserRounding(\"HALF_UP\") or "
                + "set the system property avro.decimal.defaultDeserRounding=HALF_UP ");
      }
    }
    if (result.precision() > precision) {
          throw new AvroRuntimeException("Received Decimal " + object + " is not compatible with precision " + precision
                + " if you desire rounding, you can annotate type with @deserRounding(\"HALF_UP\") or "
                + "set the system property avro.decimal.defaultDeserRounding=HALF_UP ");
    }
    return result;
  }

  public abstract  Object doSerialize(BigDecimal decimal);


  @Override
  public Object serialize(BigDecimal decimal) {
    if (decimal.scale() > scale) {
      if (serRm != null) {
        decimal = decimal.setScale(scale, serRm);
      } else {
        throw new UnsupportedOperationException("Decimal " + decimal + " exceeds scale " + scale
                + " if you desire rounding, you can annotate type with @serRounding(\"HALF_UP\") or "
                + "set the system property avro.decimal.defaultSerRounding=HALF_UP ");
      }
    }
    if (decimal.precision() > precision) {
      throw new UnsupportedOperationException("Decimal " + decimal + " exceeds precision " + precision);
    }
    return doSerialize(decimal);
  }

  @Override
  public BigDecimal tryDirectDecode(Decoder dec, final Schema schema) throws IOException {
    if (dec instanceof JsonExtensionDecoder) {
      return ((JsonExtensionDecoder) dec).readBigDecimal(schema);
    } else {
      return null;
    }
  }

  @Override
  public boolean tryDirectEncode(BigDecimal object, Encoder enc, final Schema schema) throws IOException {
    if (DecimalEncoder.OPTIMIZED_JSON_DECIMAL_WRITE && enc instanceof JsonExtensionEncoder) {
      ((JsonExtensionEncoder) enc).writeDecimal(object, schema);
      return true;
    } else {
      return false;
    }
  }

}
