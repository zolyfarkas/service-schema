
package org.apache.avro.reflect;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import org.apache.avro.AvroTypeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * @author Zoltan Farkas
 */
public class ExtendedReflectData extends ReflectData {

  private static final ExtendedReflectData INSTANCE = new ExtendedReflectData();

  private static final Schema NONE = Schema.create(Schema.Type.NULL);

  private final ConcurrentMap<Type, Schema> simpleMap = new ConcurrentHashMap<>();

  public static ExtendedReflectData get() {
    return INSTANCE;
  }

  @Override
  @Nullable
  public Schema getSchema(final Type type) {
    Schema result = simpleMap.computeIfAbsent(type, (t) -> {
      try {
        return createSchema(t, new HashMap<>());
      } catch (RuntimeException ex) {
        return NONE;
      }
    });
    return result == NONE ? null : result;
  }

  /**
   * Todo: to be cached.
   * @param type
   * @param typeVarName
   * @param typeVarType
   * @return
   */
  public Schema getSchema(final Type type, String typeVarName, Type typeVarType) {
    Map<String, Schema> typeMap = new HashMap<>();
    typeMap.put(typeVarName, getSchema(typeVarType));
    return createSchema(type, typeMap);
  }


  @Override
  public Schema createSchema(Type type, Map<String, Schema> names) {
    if (type instanceof TypeVariable) {
      String name = ((TypeVariable) type).getName();
      Schema schema = names.get(name);
      if (schema == null) {
        throw new IllegalArgumentException("Undefined type variable " + type);
      }
      return schema;
    }
    return super.createSchema(type, names);
  }


  public Schema createSchema(Type type, Object object, Map<String, Schema> names) {
    if (type instanceof TypeVariable) {
      String name = ((TypeVariable) type).getName();
      Schema schema = names.get(name);
      if (schema == null) {
        throw new IllegalArgumentException("Undefined type variable " + type);
      }
      return schema;
    }

    if (type instanceof GenericArrayType) {                  // generic array
      Type component = ((GenericArrayType)type).getGenericComponentType();
      if (component == Byte.TYPE)                            // byte array
        return Schema.create(Schema.Type.BYTES);
      Schema result;
      if (java.lang.reflect.Array.getLength(object) > 0) {
        result = Schema.createArray(createSchema(component, java.lang.reflect.Array.get(object, 0), names));
        setElement(result, component);
      } else {
        result = Schema.createArray(Schema.create(Schema.Type.NULL));
        setElement(result, component);
      }
      return result;
    } else if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType)type;
      Class raw = (Class)ptype.getRawType();
      Type[] params = ptype.getActualTypeArguments();
      if (Map.class.isAssignableFrom(raw)) {  // Map
        Map map = (Map) object;
        Class key = (Class)params[0];
        if (isStringable(key)) {                             // Stringable key
          Schema schema;
          if (map.isEmpty()) {
            schema = Schema.createMap(Schema.create(Schema.Type.NULL));
          } else {
            schema = Schema.createMap(createSchema(params[1], map.values().iterator().next(), names));
          }
          schema.addProp(SpecificData.CLASS_PROP, key.getName());
          return schema;
        } else if (key != String.class) {
          Schema schema;
          if (map.isEmpty()) {
            schema = Schema.createMap(Schema.create(Schema.Type.NULL));
          } else {
            Map.Entry next = (Map.Entry) map.entrySet().iterator().next();
            schema = createNonStringMapSchema(next.getKey().getClass(), next.getKey().getClass(), names);
          }
          schema.addProp(SpecificData.CLASS_PROP, raw.getName());
          return schema;
        }
      } else if (Collection.class.isAssignableFrom(raw)) {   // Collection
        Collection col = (Collection) object;
        if (col.size() > 0) {
          Schema schema = Schema.createArray(createSchema(params[0], col.iterator().next(), names));
          schema.addProp(SpecificData.CLASS_PROP, raw.getName());
          return schema;
        } else {
          Schema schema = Schema.createArray(Schema.create(Schema.Type.NULL));
          schema.addProp(SpecificData.CLASS_PROP, raw.getName());
          return schema;
        }
      }
    } else if ((type == Byte.class) || (type == Byte.TYPE)) {
      Schema result = Schema.create(Schema.Type.INT);
      result.addProp(SpecificData.CLASS_PROP, Byte.class.getName());
      return result;
    } else if ((type == Short.class) || (type == Short.TYPE)) {
      Schema result = Schema.create(Schema.Type.INT);
      result.addProp(SpecificData.CLASS_PROP, Short.class.getName());
      return result;
    } else if ((type == Character.class) || (type == Character.TYPE)) {
        Schema result = Schema.create(Schema.Type.INT);
        result.addProp(SpecificData.CLASS_PROP, Character.class.getName());
        return result;
    } else if (type instanceof Class) {                      // Class
      Class<?> c = (Class<?>)type;
      if (c.isPrimitive() ||                                 // primitives
          c == Void.class || c == Boolean.class ||
          c == Integer.class || c == Long.class ||
          c == Float.class || c == Double.class ||
          c == Byte.class || c == Short.class ||
          c == Character.class)
        return super.createSchema(type, names);
      if (c.isArray()) {                                     // array
        Class component = c.getComponentType();
        if (component == Byte.TYPE) {                        // byte array
          Schema result = Schema.create(Schema.Type.BYTES);
          result.addProp(SpecificData.CLASS_PROP, c.getName());
          return result;
        }
        Schema result = Schema.createArray(createSchema(component, names));
        result.addProp(SpecificData.CLASS_PROP, c.getName());
        setElement(result, component);
        return result;
      }
      AvroSchema explicit = c.getAnnotation(AvroSchema.class);
      if (explicit != null)                                  // explicit schema
        return Schema.parse(explicit.value());
      if (CharSequence.class.isAssignableFrom(c))            // String
        return Schema.create(Schema.Type.STRING);
      if (ByteBuffer.class.isAssignableFrom(c))              // bytes
        return Schema.create(Schema.Type.BYTES);
      if (Collection.class.isAssignableFrom(c))  {
        Collection col = (Collection) object;
        if (col.size() > 0) {
          Object next = col.iterator().next();
          return Schema.createArray(createSchema(next.getClass(), next, names));
        } else {
          return Schema.createArray(Schema.create(Schema.Type.NULL));
        }
      }
      if (Map.class.isAssignableFrom(c)) {
        Map map = (Map) object;
        if (map.isEmpty()) {
          return Schema.createMap(Schema.create(Schema.Type.NULL));
        }
        Map.Entry elem = (Map.Entry) map.entrySet().iterator().next();
        Class key = elem.getKey().getClass();
        if (isStringable(key) || CharSequence.class.isAssignableFrom(key)) {   // Stringable key
          Schema schema = Schema.createMap(createSchema(elem.getValue().getClass(), elem.getValue(), names));
          if (key != String.class) {
            schema.addProp(SpecificData.CLASS_PROP, key.getName());
          }
          return schema;
        } else if (key != String.class) {
          Schema schema = createNonStringMapSchema(elem.getKey().getClass(), elem.getValue().getClass(), names);
          return schema;
        }
      }
      String fullName = c.getName();
      Schema schema = names.get(fullName);
      if (schema == null) {
        AvroDoc annotatedDoc = c.getAnnotation(AvroDoc.class);    // Docstring
        String doc = (annotatedDoc != null) ? annotatedDoc.value() : null;
        String name = c.getSimpleName();
        String space = c.getPackage() == null ? "" : c.getPackage().getName();
        if (c.getEnclosingClass() != null)                   // nested class
          space = c.getEnclosingClass().getName();
        Union union = c.getAnnotation(Union.class);
        if (union != null) {                                 // union annotated
          return getAnnotatedUnion(union, names);
        } else if (isStringable(c)) {                        // Stringable
          Schema result = Schema.create(Schema.Type.STRING);
          result.addProp(SpecificData.CLASS_PROP, c.getName());
          return result;
        } else if (c.isEnum()) {                             // Enum
          Enum[] constants = (Enum[])c.getEnumConstants();
          List<String> symbols = new ArrayList<>(constants.length);
          for (int i = 0; i < constants.length; i++) {
            symbols.add(constants[i].name());
          }
          schema = Schema.createEnum(name, doc, space, symbols);
          consumeAvroAliasAnnotation(c, schema);
        } else if (GenericFixed.class.isAssignableFrom(c)) { // fixed
          int size = c.getAnnotation(FixedSize.class).value();
          schema = Schema.createFixed(name, doc, space, size);
          consumeAvroAliasAnnotation(c, schema);
        } else if (IndexedRecord.class.isAssignableFrom(c)) { // specific
          if (SpecificRecordBase.class.isAssignableFrom(c)) {
            return super.createSchema(type, names);
          } else {
            return ((GenericRecord) object).getSchema();
          }
        } else {                                             // record
          List<Schema.Field> fields = new ArrayList<>();
          boolean error = Throwable.class.isAssignableFrom(c);
          schema = Schema.createRecord(name, doc, space, error);
          consumeAvroAliasAnnotation(c, schema);
          names.put(c.getName(), schema);
          for (Field field : getCachedFields(c))
            if ((field.getModifiers()&(Modifier.TRANSIENT|Modifier.STATIC))==0
                && !field.isAnnotationPresent(AvroIgnore.class)) {
              Schema fieldSchema = createFieldSchema(field, names);
              AvroDefault defaultAnnotation
                = field.getAnnotation(AvroDefault.class);
              Object defaultValue = (defaultAnnotation == null)
                ? null
                : Schema.parseJson(defaultAnnotation.value());
              annotatedDoc = field.getAnnotation(AvroDoc.class);    // Docstring
              doc = (annotatedDoc != null) ? annotatedDoc.value() : null;

              if (defaultValue == null
                  && fieldSchema.getType() == Schema.Type.UNION) {
                Schema defaultType = fieldSchema.getTypes().get(0);
                if (defaultType.getType() == Schema.Type.NULL) {
                  defaultValue = JsonProperties.NULL_VALUE;
                }
              }
              AvroName annotatedName = field.getAnnotation(AvroName.class);       // Rename fields
              String fieldName = (annotatedName != null)
                ? annotatedName.value()
                : field.getName();
              Schema.Field recordField
                = new Schema.Field(fieldName, fieldSchema, doc, defaultValue);

              AvroMetas metas = field.getAnnotation(AvroMetas.class);
              if (metas != null) {
                for (AvroMeta meta : metas.value()) {
                 recordField.addProp(meta.key(), meta.value());
                }
              }
              AvroMeta meta = field.getAnnotation(AvroMeta.class);              // add metadata
              if (meta != null)
                recordField.addProp(meta.key(), meta.value());
              for(Schema.Field f : fields) {
                if (f.name().equals(fieldName))
                  throw new AvroTypeException("double field entry: "+ fieldName);
              }

              consumeFieldAlias(field, recordField);

              fields.add(recordField);
            }
          if (error)                              // add Throwable message
            fields.add(new Schema.Field("detailMessage", THROWABLE_MESSAGE,
                                        null, null));
          schema.setFields(fields);
          AvroMetas metas = c.getAnnotation(AvroMetas.class);
          if (metas != null) {
            for (AvroMeta meta : metas.value()) {
              schema.addProp(meta.key(), meta.value());
            }
          }
          AvroMeta meta = c.getAnnotation(AvroMeta.class);
          if (meta != null)
              schema.addProp(meta.key(), meta.value());
        }
        names.put(fullName, schema);
      }
      return schema;
    }
    return super.createSchema(type, names);
  }

}
