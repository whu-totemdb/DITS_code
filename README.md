# DITS
Introduction
-------------
This repo holds the source code and scripts for reproducing the key experiments of 'Joinable Search over Multi-source Spatial Datasets: Overlap, Coverage, and Efficiency'.

Requirements
============
* JDK == 1.8
* At least two devices


Usage
============
Dependency
-----------
* Please download the 'Quadtree' folder from the above './mvn_denpendency/Quadtree/' directory and put it in your maven repository
* this 'Quadtree' dependency provides the implementation of the comparison index "quadtree".

Parameter setting
-----------
* clientSize: set the number of data sources in interactionCenter/src/main/java/Server/interactionCenter.java
* IPaddress: set the IP address of the interaction center device in dataSource/src/main/java/Client/dataSource.java

Run experiment
----------
* Run the interactionCenter module on one device first
* After running interactionCenter, run the dataSource module on the other device
