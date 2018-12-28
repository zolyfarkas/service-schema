package org.apache.avro;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author Zoltan Farkas
 */
public interface SchemaResolver {

  @Nonnull
  Schema resolveSchema(String id);

  @Nullable
  String getId(Schema schema);

}
