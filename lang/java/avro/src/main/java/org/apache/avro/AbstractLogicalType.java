package org.apache.avro;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.avro.util.internal.JacksonUtils;
import org.codehaus.jackson.node.TextNode;

public abstract class AbstractLogicalType<T> extends JsonProperties implements LogicalType<T> {

  protected AbstractLogicalType(Schema.Type type, Set<String> reserved, String logicalTypeName,
          Map<String, Object> properties, Class<T> javaClasZ) {
    super(reserved);
    this.properties = new HashMap<String, Object>(properties);
    for (Map.Entry<String, Object> prop : properties.entrySet()) {
      props.put(prop.getKey(), JacksonUtils.toJsonNode(prop.getValue()));
    }
    this.properties.put("logicalType", logicalTypeName);
    props.put("logicalType", TextNode.valueOf(logicalTypeName));
    this.logicalTypeName = logicalTypeName;
    this.type = type;
    this.javaClasZ = javaClasZ;
  }

  protected final String logicalTypeName;

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
    return this.props.equals(other.props);
  }

  @Override
  public final Class<T> getLogicalJavaType() {
    return javaClasZ;
  }

  @Override
  public int hashCode() {
    return logicalTypeName.hashCode() + 7 * props.hashCode();
  }

  public String getName() {
    return logicalTypeName;
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

  @Override
  public void addToSchema(Schema schema) {
    schema.setLogicalType(this);
  }

}
