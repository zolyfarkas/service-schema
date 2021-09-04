/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.avro.util.internal;

import org.apache.avro.Schema;

/**
 * @author Zoltan Farkas
 */
public class UnresolvedSchemas {

  public static final String UR_SCHEMA_ATTR = "org.apache.avro.compiler.idl.unresolved.name";

  public static final String UR_SCHEMA_NAME = "UnresolvedSchema";

  public static final String UR_SCHEMA_NS = "org.apache.avro.compiler";

  public static boolean isUnresolvedSchema(final Schema schema) {
    return (schema.getType() == Schema.Type.RECORD && schema.getProp(UR_SCHEMA_ATTR) != null
        && schema.getName().startsWith(UR_SCHEMA_NAME)
        && UR_SCHEMA_NS.equals(schema.getNamespace()));
  }

}
