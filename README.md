Apache Avro™ is a data serialization system.

Learn more about Avro, please visit our website at:

  http://avro.apache.org/

Features implemented in this fork on the java side:

 1) ExtendedJsonDecoder/Encoder -  encodes unions of null and a single type as a more normal key=value rather than key={type=value}.
 ExtendedJsonDecoder will infer missing fields using the default values from the write'r schema and as such need not be present in the serialized json. The ExtendedGenericDatumWriter can be used to omit serializing fields that equal the  default values defined in the schema. This can make the json payload smaller than the binary payload in some use cases.

 2) Evolvable enums, with custom string values, where additionally you can rename enum values to maintain backward compatibility. Example:

```
@symbolAliases({"DIAMONDS" : ["Bling Bling", "ROCKS"]})
@stringSymbols({"SPADES" : "S P A D E S"})
@fallbackSymbol("UNKNOWN")
 enum Suit {
  UNKNOWN, SPADES, DIAMONDS, CLUBS, HEARTS
}
```

 3) Generated java classes are annotated with @Nullable or @Nonnull as appropriate. As such compiling in conjuction with tools like spotbugs or google error-prone will result in better quality code.

 4) @Immutable record support. (setters will not be generated)

 5) Numerous bug fixes and performance enhancements.

 6) Enhanced decimal logical type support, where JSON serialization is a proper number. (not bytes/string)

 7) json_record and json_array logical type support.

 8) @deprecated support for record and fields.

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

