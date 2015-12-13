#  Flume-NG-Gemfire-Sink

This is a [Flume](https://flume.apache.org) Sink implementation that can publish data to a [Gemfire](http://Gemfire.apache.org) region.


## Dependency Versions
- Apache Flume - 1.6.0
- Apache Gemfire - 8.2.0

## Prerequisites
- Java 1.6 or higher
- [Apache Maven 3](http://maven.apache.org)
- An [Pivotal Gemfire](http://pivotal.io/big-data/pivotal-gemfire) installation (See the dependent version above)
- An [Apache Gemfire](http://Gemfire.apache.org) installation (See the dependent version above)

## Building the project
[Apache Maven](http://maven.apache.org) is used to build the project. This [page](http://maven.apache.org/download.cgi) contains the download links and an installation guide for various operating systems.

Issue the command: > mvn clean install

This will compile the project and the binary distribution(flume-Gemfire-sink-dist-x.x.x-bin.zip) will be copied into '${project_root}/dist/target' directory.

## Setting up

1. Build the project as per the instructions in the previous subsection.
2. Unzip the binary distribution(flume-gemfire-sink-dist-x.x.x-bin.zip) inside ${project_root}/dist/target.
3. There are two ways to include this custom sink in Flume binary installation.

_Recommended Approach_
- Create a new directory inside `plugins.d` directory which is located in `${FLUME_HOME}`. If the `plugins.d` directory is not there, go ahead and create it. We will call this new directory that was created inside plugins.d 'Gemfire-sink'. You can give it any name depending on the naming conventions you prefer.
- Inside this new directory (Gemfire-sink) create two subdirectories called `lib` and `libext`.
- You can find the jar files for this sink inside the `lib` directory of the extracted archive. Copy `flume-Gemfire-sink-impl-x.x.x.jar` into the `plugins.d/Gemfire-sink/lib` directory. Then copy the rest of the jars into the `plugins.d/Gemfire-sink/libext` directory.

This is how it'll look like at the end.
```
${FLUME_HOME}
 |-- plugins.d
 		|-- Gemfire-sink
 			|-- lib
   				|-- flume-Gemfire-sink-impl-x.x.x.jar
 			|-- libext
   				|-- Gemfire_x.x.-x.x.x.x.jar
   				|-- metrics-core-x.x.x.jar
   				|-- scala-library-x.x.x.jar
```
More details can be found in the [Flume user guide](https://flume.apache.org/FlumeUserGuide.html#the-plugins-d-directory).

**OR**
                                
_Quick and Dirty Approach_
- Copy the jar files inside the `lib` directory of extracted archive into `${FLUME_HOME}/lib`.

**Deploy on Gemfire**
_deploy your jar into gemfire (like flume-gemfire-sink-example-x.x.x.jar), otherwise gemfire will complain about MessageWrapper class not found_

## Configuration
Following parameters are supported at the moment.

- **type**
	- The sink type. This should be set as `com.smarthub.flume.sink.GemfireSink`.

- **region**[optional] 
	- The region in Gemfire to which the messages will be published. If this region is mentioned, every message will be published to the same region.If region is not provided, the messages will be published to a default region called `default-flume-region`.

- **preprocessor**[optional]
	- This is an extension point provided support dynamic regions and keys. Also it's possible to use it to support message modification before publishing to Gemfire. The full qualified class name of the preprocessor implementation should be provided here. Refer the next subsection to read more about preprocessors. If a preprocessor is not configured, then a static region should be used as explained before. And the messages will not be keyed. In a primitive setup, configuring a static region would suffice.

-- **wrapper**
	- the wrapper object class for message
	
- **Gemfire Properties**
	- These properties are used to configure the Gemfire. Any producer property supported by Gemfire can be used. The only requirement is to prepend the property name with the prefix `Gemfire.`. For instance, the `mcast-port` property should be written as `gemfire.mcast-port`. Please take a look at the [sample configuration](https://github.com/yuenengfanhua/flume-ng-gemfire-sink/blob/master/impl/src/test/resources/gemfire.properties) provided in the `conf` directory of the distribution.

	
## Implementing a messagewrapper
Implementing a message wrapper to wrap the string message to object, use wrap method
	
## Implementing a preprocessor
Implementing a custom preprocessor is useful to support dynamic keys. Also they support message transformations. The requirement is to implement the interface `com.smarthub.flume.sink.MessagePreprocessor`. The java-docs of this interface provides a detailed description of the methods, parameters, etc. There are three methods that needs to be implemented. The method names are self explainatory.

- ```public String extractKey(Event event, Context context)```
- ```public MessageWrapper transformMessage(Event event, Context context)```

The class '[com.smarthub.flume.sink.example.SimpleMessagePreprocessor](https://github.com/yuenengfanhua/flume-ng-gemfire-sink/blob/master/example/src/main/java/com/smarthub/flume/sink/example/SimpleMessagePreprocessor.java)' inside the 'example' module is an example implementation of a preprocessor.

After implementing the preprocessor, compile it into a jar and add into the Flume classpath with the rest of the jars (copy to `libext` if you are using the `plugins.d` directory or copy it to `${FLUME_HOME}\lib` if you are using the other approach) and configure the `preprocessor` parameter with its fully qualified classname. For instance;

`a1.sinks.k1.preprocessor = com.smarthub.flume.sink.example.SimpleMessagePreprocessor`

## Questions and Feedback
Please file a bug or contact me via [email](mailto:yuenengfanhua@gmail.com) with respect to any bug you encounter or any other feedback.

