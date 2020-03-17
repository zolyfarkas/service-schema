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
package org.apache.avro.io.parsing;

import java.util.Objects;
import org.apache.avro.Schema;

/**
 *
 * @author Zoltan Farkas
 */
public final class RWSchemas {

  private final Schema writer;

  private final Schema reader;

  public RWSchemas(final Schema writer, final Schema reader) {
    this.writer = writer;
    this.reader = reader;
  }

  public Schema getWriter() {
    return writer;
  }

  public Schema getReader() {
    return reader;
  }

  @Override
  public int hashCode() {
    return 97 * Objects.hashCode(this.writer) + Objects.hashCode(this.reader);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final RWSchemas other = (RWSchemas) obj;
    if (!Objects.equals(this.writer, other.writer)) {
      return false;
    }
    return Objects.equals(this.reader, other.reader);
  }

  @Override
  public String toString() {
    return "RWSchemas{" + "writer=" + writer + ", reader=" + reader + '}';
  }

}
