# DITS
Introduction
-------------
This repo holds the source code and scripts for reproducing the key experiments of 'Joinable Search over Multi-source Spatial Datasets: Overlap, Coverage, and Efficiency', which studies two cases of joinable search problems from multiple spatial data sources.

Technical Report
============
https://arxiv.org/pdf/2311.13383

Requirements
============
* JDK == 1.8
* At least two devices

Datasets
============
* We conduct experiments on the following five data sources, each of which is downloaded from an open-source spatial data portal.
* Baidu-dataset is collected from the Baidu Maps Open Platform, which contains spatial datasets from different industry
categories for 28 cities in China (https://lbsyun.baidu.com/). 
*  BTAA-dataset is collected from the Big Ten Academic Alliance Geoportal, which contains spatial data for the midwestern US, such as Illinois, Indiana, and Michigan (https://geo.btaa.org/).
* NYU-dataset is collected from the NYU Spatial Data Repository, which contains geographic information on multisubjects like census and transportation worldwide (https://geo.nyu.edu/).
* Transit-dataset is collected from Big Geoportal, which contains different kinds of traffic data from Maryland and
Washington D.C., such as buses, metro, and waterways (https://geo.btaa.org/).
* UMN-dataset is collected from the data repository of Minnesota University, which contains geographic information
from various topics like agriculture and ecology (https://conservancy.umn.edu/drum).


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

Citation
---------
* If you use our code for research work, please cite our paper below:

```
@inproceedings{Yang2025DITS,

  title={Joinable Search over Multi-source Spatial Datasets: Overlap, Coverage, and Efficiency},

  author={Yang, Wenzhe and Wang, Sheng and Chen, Zhiyu and Sun, Yuan and Peng, Zhiyong},

  booktitle={Proceedings of the 41th International Conference on Data Engineering},

  year={2025},

  publisher={IEEE},

  organization={IEEE}

}

