
package org.apache.avro;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.function.Function;

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
  public boolean customWrite(Schema schema, JsonGenerator gen) throws IOException {
    return this.sResolver.customWrite(schema, gen);
  }

  @Override
  public Schema customRead(Function<String, JsonNode> object) {
    return this.sResolver.customRead(object);
  }


  @Override
  public String toString() {
    return "AvroNamesRefResolver{" + "sResolver=" + sResolver + '}';
  }

}
