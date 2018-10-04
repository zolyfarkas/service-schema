package org.apache.avro;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

@ParametersAreNonnullByDefault
public interface LogicalType<T>  {

  String LOGICAL_TYPE_PROP = "logicalType";

  /**
   * @return the name of the logical type.
   */
  String getName();

  /** Return the set of properties that a reserved for this type */
  Set<String> reserved();

  Object getProperty(String propertyName);

  Map<String, Object> getProperties();

  /** get java type */
  Class<T> getLogicalJavaType();

  /**
   * convert from the avro type -> logical type.
   * @param object
   * @return
   */
  T deserialize(Object object);

  /**
   * convert from logicalType to the avro type.
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
  default T tryDirectDecode(final Decoder enc, final Schema schema) throws IOException{
    return null;
  }

  default String getLogicalTypeName() {
    return getName();
  }

  void addToSchema(Schema schema);

}
