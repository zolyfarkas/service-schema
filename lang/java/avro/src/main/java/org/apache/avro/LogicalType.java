package org.apache.avro;

import javax.annotation.ParametersAreNonnullByDefault;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;

/**
 * Avro Logical type interface.
 *
 * @author Zoltan Farkas
 */
@ParametersAreNonnullByDefault
public class LogicalType {

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

}
