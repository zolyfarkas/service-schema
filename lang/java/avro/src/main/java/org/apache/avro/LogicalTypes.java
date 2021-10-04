/*
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

package org.apache.avro;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.logical_types.factories.AnyAvroLogicalTypeFactory;
import org.apache.avro.logical_types.factories.TemporalLogicalTypeFactory;
import org.apache.avro.logical_types.factories.BigIntegerFactory;
import org.apache.avro.logical_types.factories.DateLogicalTypeFactory;
import org.apache.avro.logical_types.factories.DecimalFactory;
import org.apache.avro.logical_types.factories.DurationLogicalTypeFactory;
import org.apache.avro.logical_types.factories.InstantLogicalTypeFactory;
import org.apache.avro.logical_types.factories.JsonAnyLogicalTypeFactory;
import org.apache.avro.logical_types.factories.JsonArrayLogicalTypeFactory;
import org.apache.avro.logical_types.factories.JsonRecordLogicalTypeFactory;
import org.apache.avro.logical_types.factories.LocalTimestampMicrosLogicalTypeFactory;
import org.apache.avro.logical_types.factories.LocalTimestampMillisLogicalTypeFactory;
import org.apache.avro.logical_types.factories.RegexpLogicalTypeFactory;
import org.apache.avro.logical_types.factories.SchemaLogicalTypeFactory;
import org.apache.avro.logical_types.factories.TimeMicrosLogicalTypeFactory;
import org.apache.avro.logical_types.factories.TimeMillisLogicalTypeFactory;
import org.apache.avro.logical_types.factories.TimestampMicrosLogicalTypeFactory;
import org.apache.avro.logical_types.factories.TimestampMillisLogicalTypeFactory;
import org.apache.avro.logical_types.factories.URILogicalTypeFactory;
import org.apache.avro.logical_types.factories.URLLogicalTypeFactory;
import org.apache.avro.logical_types.factories.UuidLogicalTypeFactory;


public class LogicalTypes {


  private static final String DECIMAL = "decimal";
  private static final String UUID = "uuid";
  private static final String DATE = "date";
  private static final String TIME_MILLIS = "time-millis";
  private static final String TIME_MICROS = "time-micros";
  private static final String TIMESTAMP_MILLIS = "timestamp-millis";
  private static final String TIMESTAMP_MICROS = "timestamp-micros";
  private static final String LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis";
  private static final String LOCAL_TIMESTAMP_MICROS = "local-timestamp-micros";


  private static final LogicalType UUID_TYPE = new LogicalType(UUID);
  private static final Date DATE_TYPE = new Date();
  private static final TimeMillis TIME_MILLIS_TYPE = new TimeMillis();
  private static final TimeMicros TIME_MICROS_TYPE = new TimeMicros();
  private static final TimestampMillis TIMESTAMP_MILLIS_TYPE = new TimestampMillis();
  private static final TimestampMicros TIMESTAMP_MICROS_TYPE = new TimestampMicros();
  private static final LocalTimestampMillis LOCAL_TIMESTAMP_MILLIS_TYPE = new LocalTimestampMillis();
  private static final LocalTimestampMicros LOCAL_TIMESTAMP_MICROS_TYPE = new LocalTimestampMicros();


  public interface LogicalTypeFactory {
    LogicalType fromSchema(Schema schema);

    default String getTypeName() {
      throw new UnsupportedOperationException();
    }
  }

  /** Create a Decimal LogicalType with the given precision and scale 0 */
  public static Decimal decimal(int precision) {
    return decimal(precision, 0);
  }

  /** Create a Decimal LogicalType with the given precision and scale */
  public static Decimal decimal(int precision, int scale) {
    return new Decimal(precision, scale);
  }

  private static final Map<String, LogicalTypeFactory> REGISTERED_TYPES =
      new ConcurrentHashMap<>();


  static {
     register(new DecimalFactory());
     register(new BigIntegerFactory());
     register(new JsonRecordLogicalTypeFactory());
     register(new JsonArrayLogicalTypeFactory());
     register(new JsonAnyLogicalTypeFactory());
     register(new AnyAvroLogicalTypeFactory());
     register(new TemporalLogicalTypeFactory());
     register(new DateLogicalTypeFactory());
     register(new InstantLogicalTypeFactory());
     register(new UuidLogicalTypeFactory());
     register(new URLLogicalTypeFactory());
     register(new URILogicalTypeFactory());
     register(new DurationLogicalTypeFactory());
     register(new RegexpLogicalTypeFactory());
     register(new SchemaLogicalTypeFactory());
     register(new TimestampMillisLogicalTypeFactory());
     register(new TimestampMicrosLogicalTypeFactory());
     register(new TimeMicrosLogicalTypeFactory());
     register(new TimeMillisLogicalTypeFactory());
     register(new LocalTimestampMicrosLogicalTypeFactory());
     register(new LocalTimestampMillisLogicalTypeFactory());
     ServiceLoader<LogicalTypeFactory> factories
             = ServiceLoader.load(LogicalTypeFactory.class);
     Iterator<LogicalTypeFactory> iterator = factories.iterator();
     while (iterator.hasNext()) {
        register(iterator.next());
     }
     Logger.getLogger("avro.LogicalTypes").log(Level.FINE, "LogicalTypes loaded {0}", REGISTERED_TYPES.keySet());
  }

  public static LogicalType uuid() {
    return UUID_TYPE;
  }

  public static LogicalType date() {
    return DATE_TYPE;
  }

  public static TimeMillis timeMillis() {
    return TIME_MILLIS_TYPE;
  }

  public static TimeMicros timeMicros() {
    return TIME_MICROS_TYPE;
  }

  public static TimestampMillis timestampMillis() {
    return TIMESTAMP_MILLIS_TYPE;
  }

  public static TimestampMicros timestampMicros() {
    return TIMESTAMP_MICROS_TYPE;
  }

  public static LocalTimestampMillis localTimestampMillis() {
    return LOCAL_TIMESTAMP_MILLIS_TYPE;
  }

  public static LocalTimestampMicros localTimestampMicros() {
    return LOCAL_TIMESTAMP_MICROS_TYPE;
  }


  /** Decimal represents arbitrary-precision fixed-scale decimal numbers */
  public static class Decimal extends LogicalType {
    private static final String PRECISION_PROP = "precision";
    private static final String SCALE_PROP = "scale";

    private final int precision;
    private final int scale;

    private Decimal(int precision, int scale) {
      super(DECIMAL);
      this.precision = precision;
      this.scale = scale;
    }

    private Decimal(Schema schema) {
      super("decimal");
      if (!hasProperty(schema, PRECISION_PROP)) {
        throw new IllegalArgumentException("Invalid decimal: missing precision");
      }

      this.precision = getInt(schema, PRECISION_PROP);

      if (hasProperty(schema, SCALE_PROP)) {
        this.scale = getInt(schema, SCALE_PROP);
      } else {
        this.scale = 0;
      }
    }

    @Override
    public Schema addToSchema(Schema schema) {
      super.addToSchema(schema);
      schema.addProp(PRECISION_PROP, precision);
      schema.addProp(SCALE_PROP, scale);
      return schema;
    }

    public int getPrecision() {
      return precision;
    }

    public int getScale() {
      return scale;
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      // validate the type
      if (schema.getType() != Schema.Type.FIXED && schema.getType() != Schema.Type.BYTES) {
        throw new IllegalArgumentException("Logical type decimal must be backed by fixed or bytes");
      }
      if (precision <= 0) {
        throw new IllegalArgumentException("Invalid decimal precision: " + precision + " (must be positive)");
      } else if (precision > maxPrecision(schema)) {
        throw new IllegalArgumentException("fixed(" + schema.getFixedSize() + ") cannot store " + precision
            + " digits (max " + maxPrecision(schema) + ")");
      }
      if (scale < 0) {
        throw new IllegalArgumentException("Invalid decimal scale: " + scale + " (must be positive)");
      } else if (scale > precision) {
        throw new IllegalArgumentException(
            "Invalid decimal scale: " + scale + " (greater than precision: " + precision + ")");
      }
    }

    private long maxPrecision(Schema schema) {
      if (schema.getType() == Schema.Type.BYTES) {
        // not bounded
        return Integer.MAX_VALUE;
      } else if (schema.getType() == Schema.Type.FIXED) {
        int size = schema.getFixedSize();
        return Math.round(Math.floor(Math.log10(2) * (8 * size - 1)));
      } else {
        // not valid for any other type
        return 0;
      }
    }

    private boolean hasProperty(Schema schema, String name) {
      return (schema.getObjectProp(name) != null);
    }

    private int getInt(Schema schema, String name) {
      Object obj = schema.getObjectProp(name);
      if (obj instanceof Integer) {
        return (Integer) obj;
      }
      throw new IllegalArgumentException(
          "Expected int " + name + ": " + (obj == null ? "null" : obj + ":" + obj.getClass().getSimpleName()));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      Decimal decimal = (Decimal) o;

      if (precision != decimal.precision)
        return false;
      return scale == decimal.scale;
    }

    @Override
    public int hashCode() {
      int result = precision;
      result = 31 * result + scale;
      return result;
    }
  }

  /** Date represents a date without a time */
  public static class Date extends LogicalType {
    private Date() {
      super(DATE);
    }
  }

  /** TimeMillis represents a time in milliseconds without a date */
  public static class TimeMillis extends LogicalType {
    private TimeMillis() {
      super(TIME_MILLIS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.INT) {
        throw new IllegalArgumentException("Time (millis) can only be used with an underlying int type");
      }
    }
  }

  /** TimeMicros represents a time in microseconds without a date */
  public static class TimeMicros extends LogicalType {
    private TimeMicros() {
      super(TIME_MICROS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Time (micros) can only be used with an underlying long type");
      }
    }
  }

  /** TimestampMillis represents a date and time in milliseconds */
  public static class TimestampMillis extends LogicalType {
    private TimestampMillis() {
      super(TIMESTAMP_MILLIS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Timestamp (millis) can only be used with an underlying long type");
      }
    }
  }

  /** TimestampMicros represents a date and time in microseconds */
  public static class TimestampMicros extends LogicalType {
    private TimestampMicros() {
      super(TIMESTAMP_MICROS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Timestamp (micros) can only be used with an underlying long type");
      }
    }
  }

  public static class LocalTimestampMillis extends LogicalType {
    private LocalTimestampMillis() {
      super(LOCAL_TIMESTAMP_MILLIS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Local timestamp (millis) can only be used with an underlying long type");
      }
    }
  }

  public static class LocalTimestampMicros extends LogicalType {
    private LocalTimestampMicros() {
      super(LOCAL_TIMESTAMP_MICROS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Local timestamp (micros) can only be used with an underlying long type");
      }
    }
  }


  public static void register(String logicalTypeName, LogicalTypeFactory factory) {
    register(logicalTypeName, factory);
  }

  public static void register(@Nonnull LogicalTypeFactory factory) {
    LogicalTypeFactory ex = REGISTERED_TYPES.put(factory.getTypeName(), factory);
    if (ex != null) {
      Logger.getLogger(LogicalTypes.class.getName())
              .log(Level.INFO, "Logical Type {0} is being overwritten with {1}", new Object [] {ex, factory});
    }
  }

  public static LogicalTypeFactory unregister(final String name) {
    return REGISTERED_TYPES.remove(name);
  }

  @Nullable
  public static LogicalType fromSchema(Schema schema) {
      return fromSchema(schema, isAllowUndefinedLogicalTypes());
  }


  public static LogicalType fromSchemaIgnoreInvalid(Schema schema) {
    return fromSchema(schema, true);
  }

  private static final ThreadLocal<Boolean> ALLOW_UNDEF_LT = new ThreadLocal<>();

  public static boolean isAllowUndefinedLogicalTypes() {
    Boolean tlb = ALLOW_UNDEF_LT.get();
    if (tlb == null) {
      return Boolean.getBoolean("allowUndefinedLogicalTypes");
    } else {
      return tlb;
    }
  }

  public static void setAllowUndefinedLogicalTypesThreadLocal(final Boolean isAllowUndefinedLogicalTypes) {
    ALLOW_UNDEF_LT.set(isAllowUndefinedLogicalTypes);
  }

  public static Boolean getAllowUndefinedLogicalTypesThreadLocal() {
    return ALLOW_UNDEF_LT.get();
  }

  /**
   * for avro official compatibility.
   */
  @Nullable
  public static LogicalType fromSchema(Schema schema, final boolean allowUndefinedLogicalTypes) {
    String typeName = (String) schema.getProp(LogicalType.LOGICAL_TYPE_PROP);
    if (typeName != null) {
      LogicalTypeFactory ltf = REGISTERED_TYPES.get(typeName);
      if (ltf != null) {
        return ltf.fromSchema(schema);
      } else {
        if (allowUndefinedLogicalTypes)  {
          return null;
        } else {
          throw new IllegalArgumentException("Undefined logical type "  + typeName + " for " + schema);
        }
      }
    } else {
      return null;
    }
  }



}
