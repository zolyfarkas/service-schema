Apache Avro™ is a data serialization system.

Learn more about Avro, please visit our website at:

  http://avro.apache.org/

Features implemented in this fork on the java side:

 1) ExtendedJsonDecoder/Encoder -  encodes unions of null and a single type as a more normal key=value rather than key={type=value}.
 ExtendedJsonDecoder will infer missing fields using the default values from the write'r schema and as such need not be present in the serialized json. The Extended[Generic|Specific]DatumWriter can be used to omit serializing fields that equal the  default values defined in the schema. This can make the json payload smaller than the binary payload in some use cases.

for
```
union {null, string} field = null;
```
instead of:
```
"field" : {"string" : "value"}
```
the ExtendedJsonEncoder will serialize:
```
"field" : "value"
```

 2) Evolvable enums, with custom string values, where additionally you can rename enum values to maintain backward compatibility. Example:

```
@symbolAliases({"DIAMONDS" : ["Bling Bling", "ROCKS"]}) // you can correct bad names backward compatibly.
@stringSymbols({"SPADES" : "S P A D E S"}) //you can have stringValues (toString()) that are not restricted to identifiers
@fallbackSymbol("UNKNOWN") // necessary for evolution. @default is also supported according to avro spec.
 enum Suit {
  UNKNOWN, SPADES, DIAMONDS, CLUBS, HEARTS
}
```

 3) Generated java classes are annotated with @Nullable or @Nonnull as appropriate. As such compiling in conjuction with tools like spotbugs or google error-prone will result in better quality code.

 4) @Immutable record support. (setters will not be generated). This encourages use of builders and constructors to build the 
 records.

 5) Numerous bug fixes and performance enhancements.

 6) Enhanced decimal logical type support, where JSON serialization is a proper number. (not bytes/string)

for
```
@logicalType("decimal") bytes field = "";
```
instead of:
```
"field" : "bytesvalue"
```
the ExtendedJsonEncoder will serialize:
```
"field" : 123.45
```

 7) json_record and json_array logical type support.
 
for
```
@logicalType("jsonRecord") string field = {};
```
Avro JsonEncoder will serialize:
```
"field" : "{ \"field\" : \"value \"}"
```
the ExtendedJsonEncoder will serialize:
```
"field" : {"field" : "value"}
```

 8) @deprecated support for record and fields. You can use @deprecated to deprecate avro records.

 [ ![Download latest](https://api.bintray.com/packages/zolyfarkas/core/avro/images/download.svg) ](https://bintray.com/zolyfarkas/core/avro/_latestVersion)

This fork is published to bintray, and you can use it by adding the repositories:

            <repositories>
                <repository>
                    <snapshots>
                        <enabled>false</enabled>
                    </snapshots>
                    <id>bintray-zolyfarkas-core</id>
                    <name>bintray</name>
                    <url>https://dl.bintray.com/zolyfarkas/core</url>
                </repository>
            </repositories>
            <pluginRepositories>
                <pluginRepository>
                    <snapshots>
                        <enabled>false</enabled>
                    </snapshots>
                    <id>bintray-zolyfarkas-core</id>
                    <name>bintray-plugins</name>
                    <url>https://dl.bintray.com/zolyfarkas/core</url>
                </pluginRepository>
            </pluginRepositories>

 and reference the avro lib as:


            <dependency>
              <groupId>org.apache.avro</groupId>
              <artifactId>avro</artifactId>
              <version>1.8.0.51p</version>
              <type>pom</type>
            </dependency>

 for more info see: https://bintray.com/zolyfarkas/core/avro

