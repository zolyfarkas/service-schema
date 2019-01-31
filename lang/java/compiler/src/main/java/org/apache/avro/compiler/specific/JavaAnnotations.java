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
package org.apache.avro.compiler.specific;

import org.apache.avro.compiler.specific.annotGenerators.CustomJavaAnnotationsGenerator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.avro.JsonProperties;
import org.apache.avro.Protocol.Message;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.compiler.specific.annotGenerators.BetaAnnotationsGenerator;
import org.apache.avro.compiler.specific.annotGenerators.DeprecationAnnotationsGenerator;
import org.apache.avro.compiler.specific.annotGenerators.NullableAnnotationsGenerator;

/**
 * @author Zoltan Farkas
 */
public final class JavaAnnotations {

  private static final List<JavaAnnotationsGenerator> GENERATORS;

  static {
    GENERATORS = new ArrayList<>();
    GENERATORS.add(new CustomJavaAnnotationsGenerator());
    GENERATORS.add(new DeprecationAnnotationsGenerator());
    GENERATORS.add(new BetaAnnotationsGenerator());
    GENERATORS.add(new NullableAnnotationsGenerator());
    ServiceLoader<JavaAnnotationsGenerator> gens = ServiceLoader.load(JavaAnnotationsGenerator.class);
    for (JavaAnnotationsGenerator jg : gens) {
      GENERATORS.add(jg);
    }
  }

  public static Set<String> generate(final SpecificCompiler compiler,
          final GenEntity entity, final JsonProperties entityProperties) {
    Schema schema = null;
    Schema respSchema = null;
    if (entityProperties instanceof Schema) {
        schema = (Schema) entityProperties;
    } else if (entityProperties instanceof Field) {
        schema = ((Field) entityProperties).schema();
    } else if (entityProperties instanceof Message) {
        schema = ((Message) entityProperties).getRequest();
        respSchema = ((Message) entityProperties).getResponse();
    }
    Iterator<JavaAnnotationsGenerator> iterator = GENERATORS.iterator();
    if (!iterator.hasNext()) {
      return Collections.EMPTY_SET;
    } else {
      JavaAnnotationsGenerator gen = iterator.next();
      Set<String> first = gen.generate(compiler, entity, entityProperties, schema, respSchema);
      if (iterator.hasNext()) {
        Set<String> result = new HashSet<>(first);
        do {
          result.addAll(iterator.next().generate(compiler, entity, entityProperties, schema, respSchema));
        } while (iterator.hasNext());
        return result;
      } else {
        return first;
      }
    }
  }

}
