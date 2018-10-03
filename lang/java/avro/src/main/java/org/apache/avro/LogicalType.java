package org.apache.avro;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

public interface LogicalType<T>  {

  /**
   * @return the name of the logical type.
   */
  String getName();

  /** Validate this logical type for the given Schema */
  void validate(Schema schema);

  /** Return the set of properties that a reserved for this type */
  Set<String> reserved();

  Object getProperty(String propertyName);

  Map<String, Object> getProperties();

  /** get java type */
  Class<T> getLogicalJavaType();

  T deserialize(Object object);

  Object serialize(T object);

  default boolean supportsDirectEncoding(final Encoder enc) {
    return false;
  }

  default boolean supportsDirectDecoding(final Decoder enc) {
    return false;
  }

  default T deserializeDirect(final Decoder object) throws IOException {
    throw new UnsupportedOperationException();
  }

  default void serializeDirect(final T object, final Encoder enc) throws IOException {
    throw new UnsupportedOperationException();
  }

  default String getLogicalTypeName() {
    return getName();
  }

  void addToSchema(Schema schema);

}
