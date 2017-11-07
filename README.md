# SparkRDF
A Spark extension for Resource Description Framework 

Docs: https://raulrc.github.io/sparkrdf/#org.uclm.alarcos.rrc.io.ReaderRDF


Examples: 
 
 Load a graph from input file: /tmp/data/*.nt
```scala
 val graph = loadGraph(sparkSession, inputFile)
 ```
 
 Expand graph in 4 levels: 
 ```scala
 val step = MockedTripleReader
 val graph = step.loadGraph(spark, inputFile)
 val depth = 4
 val result = step.expandNodesNLevel(graph.vertices, graph, depth)
```

## Future Work

* Investigate Graphframes (https://graphframes.github.io/)
* Include some new operations to RDF support