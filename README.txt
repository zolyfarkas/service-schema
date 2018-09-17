Apache Avro™ is a data serialization system.

Learn more about Avro, please visit our website at:

  http://avro.apache.org/

To contribute to Avro, please read:

  https://cwiki.apache.org/confluence/display/AVRO/How+To+Contribute

Features implemented in this fork on the java side:

 1) ExtendedJsonDecoder/Encoder -  encodes unions of null and a single type as a more normal key=value rather than key={type=value}.
 Also fields equal with the default value will be inferred from the schema and as such need not be present in the serialized json.

 2) Evolvable enums, where additionally you can rename enum values to maintain backward compatibility.

 3) Generated java classes are annotated with @Nullable or @Nonnull as appropriate.

 4) @Immutable record support. (setters will not be generated)

 5) Numerous bug fixes and performance enhancements.

 6) Enhanced decimal logical type support, where JSON serialization is a proper number. (not bytes/string)


