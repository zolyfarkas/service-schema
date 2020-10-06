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
import org.apache.avro.logicalTypes.AnyAvroLogicalTypeFactory;
import org.apache.avro.logicalTypes.AnyTemporalLogicalTypeFactory;
import org.apache.avro.logicalTypes.BigIntegerFactory;
import org.apache.avro.logicalTypes.DateIntLogicalType;
import org.apache.avro.logicalTypes.DateLogicalTypeFactory;
import org.apache.avro.logicalTypes.DecimalFactory;
import org.apache.avro.logicalTypes.InstantLogicalTypeFactory;
import org.apache.avro.logicalTypes.JsonAnyLogicalTypeFactory;
import org.apache.avro.logicalTypes.JsonArrayLogicalTypeFactory;
import org.apache.avro.logicalTypes.JsonRecordLogicalTypeFactory;
import org.apache.avro.logicalTypes.SchemaLogicalTypeFactory;
import org.apache.avro.logicalTypes.TimestampMicrosLogicalTypeFactory;
import org.apache.avro.logicalTypes.TimestampMillisLogicalTypeFactory;
import org.apache.avro.logicalTypes.URILogicalTypeFactory;
import org.apache.avro.logicalTypes.URLLogicalTypeFactory;
import org.apache.avro.logicalTypes.UuidLogicalTypeFactory;


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


  private static final Map<String, org.apache.avro.LogicalTypeFactory> REGISTERED_TYPES =
      new ConcurrentHashMap<>();


  static {
     register(new DecimalFactory());
     register(new BigIntegerFactory());
     register(new JsonRecordLogicalTypeFactory());
     register(new JsonArrayLogicalTypeFactory());
     register(new JsonAnyLogicalTypeFactory());
     register(new AnyAvroLogicalTypeFactory());
     register(new AnyTemporalLogicalTypeFactory());
     register(new DateLogicalTypeFactory());
     register(new InstantLogicalTypeFactory());
     register(new UuidLogicalTypeFactory());
     register(new URLLogicalTypeFactory());
     register(new URILogicalTypeFactory());
     register(new SchemaLogicalTypeFactory());
     register(new TimestampMillisLogicalTypeFactory());
     register(new TimestampMicrosLogicalTypeFactory());
     ServiceLoader<org.apache.avro.LogicalTypeFactory> factories
             = ServiceLoader.load(org.apache.avro.LogicalTypeFactory.class);
     Iterator<org.apache.avro.LogicalTypeFactory> iterator = factories.iterator();
     while (iterator.hasNext()) {
        register(iterator.next());
     }
     Logger.getLogger("avro.LogicalTypes").log(Level.FINE, "LogicalTypes loaded {0}", REGISTERED_TYPES.keySet());
  }

  public static LogicalType uuid() {
    return UuidLogicalTypeFactory.uuid();
  }

  private static final Date DATE_TYPE = new Date();

  public static LogicalType date() {
    return DATE_TYPE;
  }

  private static final TimeMillis TIME_MILLIS_TYPE = new TimeMillis();

  public static TimeMillis timeMillis() {
    return TIME_MILLIS_TYPE;
  }

  private static final TimeMicros TIME_MICROS_TYPE = new TimeMicros();

  public static TimeMicros timeMicros() {
    return TIME_MICROS_TYPE;
  }

  private static final TimestampMillis TIMESTAMP_MILLIS_TYPE = new TimestampMillis();

  public static TimestampMillis timestampMillis() {
    return TIMESTAMP_MILLIS_TYPE;
  }

  private static final TimestampMicros TIMESTAMP_MICROS_TYPE = new TimestampMicros();

  public static TimestampMicros timestampMicros() {
    return TIMESTAMP_MICROS_TYPE;
  }

  private static final LocalTimestampMillis LOCAL_TIMESTAMP_MILLIS_TYPE = new LocalTimestampMillis();

  public static LocalTimestampMillis localTimestampMillis() {
    return LOCAL_TIMESTAMP_MILLIS_TYPE;
  }

  private static final LocalTimestampMicros LOCAL_TIMESTAMP_MICROS_TYPE = new LocalTimestampMicros();

  public static LocalTimestampMicros localTimestampMicros() {
    return LOCAL_TIMESTAMP_MICROS_TYPE;
  }


  /** Date represents a date without a time */
  public static class Date extends DateIntLogicalType {
    private Date() {
      super(Schema.create(Schema.Type.INT));
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





  /**
   * factory for avro official compatibility.
   */
  public interface LogicalTypeFactory  {
    LogicalType fromSchema(Schema schema);
  }

  public static void register(String logicalTypeName, LogicalTypeFactory factory) {
    register(new org.apache.avro.LogicalTypeFactory() {
      @Override
      public String getLogicalTypeName() {
        return logicalTypeName;
      }

      @Override
      public LogicalType create(Schema.Type schemaType, Map<String, Object> attributes) {
        throw new UnsupportedOperationException();
      }

      @Override
      public LogicalType fromSchema(Schema schema) {
        return factory.fromSchema(schema);
      }

    });
  }

  public static void register(@Nonnull org.apache.avro.LogicalTypeFactory factory) {
    LogicalTypeFactory ex = REGISTERED_TYPES.put(factory.getLogicalTypeName(), factory);
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
      org.apache.avro.LogicalTypeFactory ltf = REGISTERED_TYPES.get(typeName);
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
