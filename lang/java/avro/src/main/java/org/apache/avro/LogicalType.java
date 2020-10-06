package org.apache.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Optional;

/**
 * Avro Logical type interface.
 *
 * @author Zoltan Farkas
 * @param <T> the primary type this logical type maps to. a conversion to/from this type to the underlying type must
 * exist.
 */
@ParametersAreNonnullByDefault
public abstract class LogicalType<T> {

  public static final String LOGICAL_TYPE_PROP = "logicalType";

  private static final String[] INCOMPATIBLE_PROPS = new String[] { GenericData.STRING_PROP, SpecificData.CLASS_PROP,
      SpecificData.KEY_CLASS_PROP, SpecificData.ELEMENT_PROP };


  private final String name;

  public LogicalType(String logicalTypeName) {
    this.name = logicalTypeName.intern();
  }

  /**
   * @return the name of the logical type.
   */
  public final String getName() {
    return name;
  }


  public Object getProperty(String propertyName) {
    return null;
  }

  public Map<String, Object> getProperties() {
    return Collections.EMPTY_MAP;
  }

  /**
   * get java type
   */
  @Nullable
  public Class<T> getLogicalJavaType() {
    return null;
  }

  public int computehashCode(T object) {
    return object.hashCode();
  }

  /**
   * convert from the avro type -> logical type.
   *
   * @param object
   * @return
   */
  public T deserialize(Object object) {
    return (T) object;
  }

  /**
   * convert from logicalType to the avro type.
   *
   * @param object
   * @return
   */
  public Object serialize(T object) {
    return object;
  }

  /**
   * @param object
   * @param enc
   * @return true if direct encoding was done.
   * @throws IOException
   */
  public boolean tryDirectEncode(T object, final Encoder enc, final Schema schema) throws IOException {
    return false;
  }

  /**
   * @param enc
   * @return null if no direct decode available.
   * @throws IOException
   */
  @Nonnull
  public Optional<T> tryDirectDecode(final Decoder enc, final Schema schema) throws IOException {
    return Optional.empty();
  }

  public String getLogicalTypeName() {
    return getName();
  }

  public Schema addToSchema(Schema schema) {
    validate(schema);
    schema.addProp(LOGICAL_TYPE_PROP, getName());
    schema.setLogicalType(this);
    return schema;
  }

  public void validate(Schema schema) {
    for (String incompatible : INCOMPATIBLE_PROPS) {
      if (schema.getProp(incompatible) != null) {
        throw new IllegalArgumentException(LOGICAL_TYPE_PROP + " cannot be used with " + incompatible);
      }
    }
  }

  public Conversion<T> getDefaultConversion() {
    return new Conversion<T>() {
      @Override
      public Class<T> getConvertedType() {
        return getLogicalJavaType();
      }

      @Override
      public String getLogicalTypeName() {
        return getName();
      }

      public T fromBoolean(Boolean value, Schema schema, LogicalType type) {
        return deserialize(value);
      }

      public T fromInt(Integer value, Schema schema, LogicalType type) {
        return deserialize(value);
      }

      public T fromLong(Long value, Schema schema, LogicalType type) {
        return deserialize(value);
      }

      public T fromFloat(Float value, Schema schema, LogicalType type) {
        return deserialize(value);
      }

      public T fromDouble(Double value, Schema schema, LogicalType type) {
        return deserialize(value);
      }

      public T fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
        return deserialize(value);
      }

      public T fromEnumSymbol(GenericEnumSymbol value, Schema schema, LogicalType type) {
        return deserialize(value);
      }

      public T fromFixed(GenericFixed value, Schema schema, LogicalType type) {
        return deserialize(value);
      }

      public T fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
        return deserialize(value);
      }

      public T fromArray(Collection<?> value, Schema schema, LogicalType type) {
        return deserialize(value);
      }

      public T fromMap(Map<?, ?> value, Schema schema, LogicalType type) {
        return deserialize(value);
      }

      public T fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
        return deserialize(value);
      }

      public Boolean toBoolean(T value, Schema schema, LogicalType type) {
        return (Boolean) serialize(value);
      }

      public Integer toInt(T value, Schema schema, LogicalType type) {
        return (Integer) serialize(value);
      }

      public Long toLong(T value, Schema schema, LogicalType type) {
        return (Long) serialize(value);
      }

      public Float toFloat(T value, Schema schema, LogicalType type) {
        return (Float) serialize(value);
      }

      public Double toDouble(T value, Schema schema, LogicalType type) {
        return (Double) serialize(value);
      }

      public CharSequence toCharSequence(T value, Schema schema, LogicalType type) {
        return (CharSequence) serialize(value);
      }

      public GenericEnumSymbol toEnumSymbol(T value, Schema schema, LogicalType type) {
        return (GenericEnumSymbol) serialize(value);
      }

      public GenericFixed toFixed(T value, Schema schema, LogicalType type) {
        return (GenericFixed) serialize(value);
      }

      public ByteBuffer toBytes(T value, Schema schema, LogicalType type) {
        return (ByteBuffer) serialize(value);
      }

      public Collection<?> toArray(T value, Schema schema, LogicalType type) {
        return (Collection<?>) serialize(value);
      }

      public Map<?, ?> toMap(T value, Schema schema, LogicalType type) {
         return (Map<?, ?>) serialize(value);
      }

      public IndexedRecord toRecord(T value, Schema schema, LogicalType type) {
         return (IndexedRecord) serialize(value);
      }

    };
  }

}
