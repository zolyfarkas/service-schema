package org.apache.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

/**
 * Avro Logical type interface.
 *
 * @author Zoltan Farkas
 * @param <T> the primary type this logical type maps to. a conversion to/from this type to the underlying type must
 * exist.
 */
@ParametersAreNonnullByDefault
public interface LogicalType<T> {

  String LOGICAL_TYPE_PROP = "logicalType";

  /**
   * @return the name of the logical type.
   */
  String getName();

  /**
   * Return the set of properties that a reserved for this type
   */
  Set<String> reserved();

  Object getProperty(String propertyName);

  Map<String, Object> getProperties();

  /**
   * get java type
   */
  Class<T> getLogicalJavaType();

  default int computehashCode(T object) {
    return object.hashCode();
  }

  /**
   * convert from the avro type -> logical type.
   *
   * @param object
   * @return
   */
  T deserialize(Object object);

  /**
   * convert from logicalType to the avro type.
   *
   * @param object
   * @return
   */
  Object serialize(T object);

  /**
   * @param object
   * @param enc
   * @return true if direct encoding was done.
   * @throws IOException
   */
  default boolean tryDirectEncode(T object, final Encoder enc, final Schema schema) throws IOException {
    return false;
  }

  /**
   * @param enc
   * @return null if no direct decode available.
   * @throws IOException
   */
  @Nullable
  default T tryDirectDecode(final Decoder enc, final Schema schema) throws IOException {
    return null;
  }

  default String getLogicalTypeName() {
    return getName();
  }

  default void addToSchema(Schema schema) {
    validate(schema);
    schema.setLogicalType(this);
  }

  default void validate(Schema schema) {
    // no validation by default.
  }

  default Conversion<T> getDefaultConversion() {
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
