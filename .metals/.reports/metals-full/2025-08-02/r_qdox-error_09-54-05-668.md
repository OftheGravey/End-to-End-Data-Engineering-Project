error id: file:///C:/Users/Ewen/Untitled%20Folder/end-to-end/flink-extractor/src/main/java/com/extractor/flink/functions/SCD2MostValidFunction.java
file:///C:/Users/Ewen/Untitled%20Folder/end-to-end/flink-extractor/src/main/java/com/extractor/flink/functions/SCD2MostValidFunction.java
### com.thoughtworks.qdox.parser.ParseException: syntax error @[11,72]

error in qdox parser
file content:
```java
offset: 471
uri: file:///C:/Users/Ewen/Untitled%20Folder/end-to-end/flink-extractor/src/main/java/com/extractor/flink/functions/SCD2MostValidFunction.java
text:
```scala
package com.extractor.flink.functions;

import java.lang.module.Configuration;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

public class SCD2MostValidFunction<Event> extends TargetDimensionRecord>@@ extends KeyedProcessFunction {
    private transient ValueState<Event> latestEventState;
    private final TypeInformation<Event> eventTypeInfo;

    protected SCD2MostValidFunction(TypeInformation<Event> eventTypeInfo) {
        this.eventTypeInfo = eventTypeInfo;
    }

    @Override
    public void open(OpenContext ctx) throws Exception {
        this.latestEventState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("latestEvent", this.eventTypeInfo)
        );
    }
}

```

```



#### Error stacktrace:

```
com.thoughtworks.qdox.parser.impl.Parser.yyerror(Parser.java:2025)
	com.thoughtworks.qdox.parser.impl.Parser.yyparse(Parser.java:2147)
	com.thoughtworks.qdox.parser.impl.Parser.parse(Parser.java:2006)
	com.thoughtworks.qdox.library.SourceLibrary.parse(SourceLibrary.java:232)
	com.thoughtworks.qdox.library.SourceLibrary.parse(SourceLibrary.java:190)
	com.thoughtworks.qdox.library.SourceLibrary.addSource(SourceLibrary.java:94)
	com.thoughtworks.qdox.library.SourceLibrary.addSource(SourceLibrary.java:89)
	com.thoughtworks.qdox.library.SortedClassLibraryBuilder.addSource(SortedClassLibraryBuilder.java:162)
	com.thoughtworks.qdox.JavaProjectBuilder.addSource(JavaProjectBuilder.java:174)
	scala.meta.internal.mtags.JavaMtags.indexRoot(JavaMtags.scala:49)
	scala.meta.internal.metals.SemanticdbDefinition$.foreachWithReturnMtags(SemanticdbDefinition.scala:99)
	scala.meta.internal.metals.Indexer.indexSourceFile(Indexer.scala:489)
	scala.meta.internal.metals.Indexer.$anonfun$reindexWorkspaceSources$3(Indexer.scala:587)
	scala.meta.internal.metals.Indexer.$anonfun$reindexWorkspaceSources$3$adapted(Indexer.scala:584)
	scala.collection.IterableOnceOps.foreach(IterableOnce.scala:619)
	scala.collection.IterableOnceOps.foreach$(IterableOnce.scala:617)
	scala.collection.AbstractIterator.foreach(Iterator.scala:1306)
	scala.meta.internal.metals.Indexer.reindexWorkspaceSources(Indexer.scala:584)
	scala.meta.internal.metals.MetalsLspService.$anonfun$onChange$2(MetalsLspService.scala:916)
	scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)
	scala.concurrent.Future$.$anonfun$apply$1(Future.scala:687)
	scala.concurrent.impl.Promise$Transformation.run(Promise.scala:467)
	java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	java.base/java.lang.Thread.run(Thread.java:1589)
```
#### Short summary: 

QDox parse error in file:///C:/Users/Ewen/Untitled%20Folder/end-to-end/flink-extractor/src/main/java/com/extractor/flink/functions/SCD2MostValidFunction.java