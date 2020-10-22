package org.apache.avro.logical_types.converters;


/**
 * @author Zoltan Farkas
 */
public class JsonArrayConversion extends JsonConversions<Object[]> {

  @SuppressWarnings("unchecked")
  public JsonArrayConversion() {
    super("json_array", (Class) Object[].class);
  }

}
