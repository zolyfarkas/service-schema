/*
 * Copyright 2019 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.compiler.specific.annotGenerators;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.GenEntity;
import org.apache.avro.compiler.specific.JavaAnnotationsGenerator;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.codehaus.jackson.JsonNode;

/**
 *
 * @author Zoltan Farkas
 */
public class CustomJavaAnnotationsGenerator implements JavaAnnotationsGenerator {

  @Override
  public Set<String> generate(SpecificCompiler compiler, GenEntity entity, JsonProperties props,
          final Schema schema, @Nullable final Schema outSchema) {
    if (entity != GenEntity.CLASS && entity != GenEntity.FIELD) {
      return Collections.EMPTY_SET;
    }
    JsonNode value = props.getJsonProp("javaAnnotation");
    if (value != null) {
      Set<String> annotations = new HashSet<>(4);
      if (value.isTextual()) {
        annotations.add(value.getTextValue());
      }
      if (value.isArray()) {
        for (JsonNode v : value) {
          annotations.add(v.getTextValue());
        }
      }
      return annotations;
    }
    return Collections.EMPTY_SET;
  }

}
