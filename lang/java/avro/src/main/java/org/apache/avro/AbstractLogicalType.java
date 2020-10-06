package org.apache.avro;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public abstract class AbstractLogicalType<T> extends LogicalType<T> {

  protected AbstractLogicalType(Schema.Type type, Set<String> reserved, String logicalTypeName,
          Map<String, Object> properties, Class<T> javaClasZ) {
    super(logicalTypeName);
    this.properties = new HashMap<String, Object>(properties);
    this.type = type;
    this.javaClasZ = javaClasZ;
  }

  protected final Map<String, Object> properties;

  protected final Schema.Type type;

  protected final Class<T> javaClasZ;

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    AbstractLogicalType other = (AbstractLogicalType) obj;
    // equal if properties are the same
    return this.properties.equals(other.properties);
  }

  @Override
  public Class<T> getLogicalJavaType() {
    return javaClasZ;
  }

  @Override
  public int hashCode() {
    return getName().hashCode() + 7 * properties.size();
  }


  public Object getProperty(String propertyName) {
    return properties.get(propertyName);
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  /** Helper method to build reserved property sets */
  public static Set<String> reservedSet(String... properties) {
    Set<String> reserved = new HashSet<String>();
    reserved.add("logicalType");
    Collections.addAll(reserved, properties);
    return reserved;
  }

}
