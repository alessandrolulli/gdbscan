NGDBSCAN
=======

We present NG-DBSCAN, an approximate density-based clustering algorithm that can operate with arbitrary similarity metrics.
The distributed design of our algorithm makes it scalable to very large datasets; its approximate nature makes it fast, yet capable of producing high quality clustering results. We provide a detailed overview of the various steps of NG-DBSCAN, together with their analysis. Our results, which we obtain through an extensive experimental campaign with real and synthetic data, substantiate our claims about NG-DBSCAN’s performance and scalability.

### Publications

**2016 - Proceedings of the VLDB Endowment**

Lulli, A., Dell'Amico, M., Michiardi, P., & Ricci, L. (2016). 
**NG-DBSCAN: scalable density-based clustering for arbitrary data.** 
Proceedings of the VLDB Endowment, 10(3), 157-168.

```
@article{lulli2016ng,
  title={NG-DBSCAN: scalable density-based clustering for arbitrary data},
  author={Lulli, Alessandro and Dell'Amico, Matteo and Michiardi, Pietro and Ricci, Laura},
  journal={Proceedings of the VLDB Endowment},
  volume={10},
  number={3},
  pages={157--168},
  year={2016},
  publisher={VLDB Endowment}
}
```

### How to build

```
mvn clean package
```

### How to configure
It is required a configuration file.
An example of configuration file can be found in "run/config_sample"

```
dataset datasetFile
outputFile outputFile

epsilon 0.8
kMax 20
k 10
maxIterations 10
coreThreshold 4

ennType PointND

terminationActiveNodes 0.7
terminationRemovedNodes 0.01
performance true

jarPath path-to-jar
sparkMaster yarn-client

edgelistSeparator \t
edgelistSeparatorCC space

sparkPartition 64
sparkExecutorInstances 1
sparkExecutorMemory 5580m
sparkCoresMax 4
```

### How to run
The main class is: "src/main/scala/enn/densityBased/ENNMainScala.scala".
To execute on a Spark cluster please follow the instruction at: http://spark.apache.org/docs/latest/