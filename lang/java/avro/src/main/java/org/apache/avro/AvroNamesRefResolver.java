
package org.apache.avro;

/**
 * @author Zoltan Farkas
 */
public final class AvroNamesRefResolver extends Schema.Names {

  private final SchemaResolver sResolver;

  public AvroNamesRefResolver(final SchemaResolver sResolver) {
    this.sResolver = sResolver;
  }

  public AvroNamesRefResolver(final SchemaResolver sClient, String space) {
    super(space);
    this.sResolver = sClient;
  }

  @Override
  public String getId(Schema schema) {
    return sResolver.getId(schema);
  }

  @Override
  public Schema resolveSchema(String id) {
    return sResolver.resolveSchema(id);
  }

  @Override
  public String getSchemaRefJsonAttr() {
    return sResolver.getJsonAttrName();
  }

  @Override
  public String toString() {
    return "AvroNamesRefResolver{" + "sResolver=" + sResolver + '}';
  }

}
