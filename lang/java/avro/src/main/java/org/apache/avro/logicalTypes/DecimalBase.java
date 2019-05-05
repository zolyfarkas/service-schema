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
import org.apache.avro.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
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

  private static final boolean SET_SCALE_WHEN_SERIALIZING =
          Boolean.parseBoolean(System.getProperty("avro.decimal.setScaleWhenSerializing", "true"));

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
  private final int precision;
  @Nullable
  private final Integer scale;
  private final MathContext serRm;
  private final MathContext deserRm;

  DecimalBase(@Nullable Number precision, @Nullable Number scale,
          Schema.Type type, @Nullable RoundingMode serRm, @Nullable RoundingMode deserRm) {
    super(type, RESERVED, "decimal", toAttributes(precision, scale, serRm, deserRm), BigDecimal.class);
    int pInt = precision == null ? 36 : precision.intValue();
    if (pInt <= 0) {
      throw new IllegalArgumentException("Invalid " + this.logicalTypeName + " precision: "
              + precision + " (must be positive)");
    }
    this.serRm = serRm == null
            ? (DEFAULT_SER_ROUNDING == null ? null : new MathContext(pInt, DEFAULT_SER_ROUNDING))
            : new MathContext(pInt, serRm);

    this.deserRm = deserRm == null
            ? (DEFAULT_DESER_ROUNDING == null ? null : new MathContext(pInt, DEFAULT_DESER_ROUNDING))
            : new MathContext(pInt, deserRm);
    mc = new MathContext(pInt, RoundingMode.HALF_EVEN);
    this.precision = pInt;
    this.scale = scale == null ? null : scale.intValue();
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
    BigDecimal result;
    if (BigDecimal.class == object.getClass()) {
      result = (BigDecimal) object;
    } else {
      result = doDeserialize(object);
    }
    result = result.stripTrailingZeros(); // reduce precission if possible.
    if (result.precision() > precision) {
      if (deserRm != null) {
        result = result.round(deserRm);
      } else {
          throw new AvroRuntimeException("Received Decimal " + object + " is not compatible with precision " + precision
                + " if you desire rounding, you can annotate type with @deserRounding(\"HALF_UP\") or "
                + "set the system property avro.decimal.defaultDeserRounding=HALF_UP ");
      }
    }
    return result;
  }

  public abstract  Object doSerialize(BigDecimal decimal);


  @Override
  public Object serialize(BigDecimal decimal) {
    if (SET_SCALE_WHEN_SERIALIZING && scale != null) {
      if (serRm != null) {
        decimal = decimal.setScale(scale, serRm.getRoundingMode());
      } else {
        decimal = decimal.setScale(scale);
      }
    }
    decimal = decimal.stripTrailingZeros();  // reduce precission if possible. (and the payload size)
    if (decimal.precision() > precision) {
      throw new UnsupportedOperationException("Decimal " + decimal + " exceeds precision " + precision);
    }
    return doSerialize(decimal);
  }

  @Override
  public Optional<BigDecimal> tryDirectDecode(Decoder dec, final Schema schema) throws IOException {
    if (dec instanceof JsonExtensionDecoder) {
      return Optional.of(((JsonExtensionDecoder) dec).readBigDecimal(schema));
    } else {
      return Optional.empty();
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
