package Client;
//11
//import Silos.MyObjectOutputStream;

import Utils.DataLoader;
import Utils.Josie;
import Utils.indexAlgorithm;
import Utils.indexNode;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.varunpant.quadtree.Node;
import com.github.varunpant.quadtree.Point;
import com.github.varunpant.quadtree.QuadTree;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import rtree.*;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;

////////////////".csv"
public class Silo {
    int dimension = 2;
    int capacity = dataSource.capacity;
    double minX = -180;
    double minY = -90;
    double rangeX = 360;
    double rangeY = 180;
    public static int resolution = 12;
    int connectedThreshold = 0;

    public indexNode dataLakeRoot;

    //    public HashMap<Integer, PriorityQueue<relaxIndexNode>> deviceResultList = new HashMap<Integer, PriorityQueue<relaxIndexNode>>();
    public PriorityQueue<relaxIndexNode> deviceResult = new PriorityQueue<>(new ComparatorByRelaxIndexNodeReverse());
    public ArrayList<indexNode> indexNodes = new ArrayList<indexNode>();
    public HashMap<String, indexNode> indexNodesHM = new HashMap<String, indexNode>();
    public HashMap<String, indexNode> datalakeRootNodes = new HashMap<>();
    public HashMap<String, HashMap<Long,Double>> zcodemap;
    public HashMap<String, double[][]> zcodeMBR = new HashMap<>();
    public ArrayList<Long> queryMapSignature = new ArrayList<>();
    public HashMap<Long, Integer> queryHistogramSignature = new HashMap<>();
    public HashMap<String, ArrayList<Long>> datasetIdMapSignature = new HashMap<>();
    public indexAlgorithm indexDSS = new indexAlgorithm();
    private  HashSet<String> candidates = new HashSet<>();
    public HashMap<Long, HashSet<String>> postinglists = new HashMap<>();
    public List<com.github.varunpant.quadtree.Node> IntersectNodeList = new ArrayList<>();
    public int topK;

    public String siloID;
    public String siloName;
    public indexNode queryNode;

    public List<String> queryNodeStringList = new ArrayList<>();

    public HashMap<String, HashMap<Integer, HashSet<Integer>>> xSliceInformation = new HashMap<>();
    public HashMap<Integer, HashSet<Integer>> querySlice;
    public HashMap<Integer, HashMap<Integer, Integer>> queryHistogramSlice;
    public String algorithm;
    String directory= dataSource.directory;
    public String preprocessFilePath = dataSource.preprocessFilePath ; // +filecountArray+"-"+silos_number;
    public String storeIndexPath = dataSource.storeIndexPath;
    public TreeMap<Long, TreeMap<String, int[]>> postingInvertedIndex = new TreeMap<>();
    public String updateFilePath;

    public boolean isLocalUpdate = false;
    public boolean isBatchUpdate = false;
    public boolean subscribeQuery = false;
    public int totalNumberOfBytes;
    public long totalNumberOfTransferTime;
    public long updateSearchTime;

    public HashMap<Integer, HashSet<Integer>> mergeQuerySlice = new HashMap<>();
    public HashSet<Long> mergeQueryZorder =  new HashSet<Long>();
    public indexNode mergeQueryIndexNode = new indexNode(dimension);

    public static int a = 6;  //   3;   //
    public static int b = 7;  //   4;  //
    public static int c = 0;

    //初始化
    public Silo(String ID){
        this.siloID = ID;
        totalNumberOfBytes = 0;
        totalNumberOfTransferTime = 0;
    }

    public Silo(String ID, String name){
        this.siloID = ID;
        this.siloName = name;
        totalNumberOfBytes = 0;
        totalNumberOfTransferTime = 0;
    }

    public Silo(String ID, String siloName, int dimension, int capacity, double minX, double minY, double rangeX,
                double rangeY, int resolution, String algo){
        this.siloID = ID;
        this.siloName = siloName;
        this.minX = minX;
        this.minY = minY;
        this.rangeX = rangeX;
        this.rangeY = rangeY;
        this.resolution = resolution;
        this.algorithm = algo;
        totalNumberOfBytes = 0;
        totalNumberOfTransferTime = 0;
    }


    public HashMap<Long, HashSet<String>> createPostinglists(HashMap<String, ArrayList<Long>> dataset) {
        HashMap<Long, HashSet<String>> postinglist = new HashMap<>();
        for(String datasetid: dataset.keySet()) {
            ArrayList<Long> signatureArrayList = dataset.get(datasetid);
            for(long code: signatureArrayList) {
                HashSet<String> datasetList;
                if(postinglist.containsKey(code))
                    datasetList = postinglist.get(code);
                else
                    datasetList = new HashSet<>();
//                if(!datasetList.contains(datasetid))
                datasetList.add(datasetid);
                postinglist.put(code, datasetList);
            }
        }
        return postinglist;
    }

    public String getRootNode(){
        String s="" ;
        s = "localIndex,"+siloID+","+
                dataLakeRoot.getMBRmax()[0]+","+dataLakeRoot.getMBRmax()[1]+","+dataLakeRoot.getMBRmin()[0]+","+dataLakeRoot.getMBRmin()[1]+","+dataLakeRoot.getPivot()[0]+","+dataLakeRoot.getPivot()[1]+","+dataLakeRoot.getRadius()+","+resolution;
//        rootNode.setMBRmax(max);
//                rootNode.setMBRmin(min);
//                rootNode.setRadius(radius);
//                rootNode.setPivot(pivot);

        return s;
    }


    public long indexCheckinData(String path)throws IOException,CloneNotSupportedException, ClassNotFoundException {
        long time = System.currentTimeMillis();
        zcodemap = generateSignatureFile(datasetIdMapSignature, datalakeRootNodes, indexNodesHM, indexNodes, zcodeMBR, dimension,
                siloID, siloName, path, minX, minY, rangeX, rangeY, resolution, xSliceInformation);
        long timee = System.currentTimeMillis();
        System.out.println("generate indexNodes = "+(timee-time));
        System.out.println("!!**" +datasetIdMapSignature.size()+", "+ indexNodesHM.size());

        //为所有的indexNodes构建ball tree
        long time1 = System.currentTimeMillis();
        indexNode parentNode = new indexNode(2);
        dataLakeRoot = indexDSS.indexDatasetKD(datasetIdMapSignature, datalakeRootNodes, parentNode, dimension,capacity,1, siloID, false);
        long time2 = System.currentTimeMillis();
        //计算内存消耗： 9个integer
        long totalIndexByte3 = 0;
        totalIndexByte3 += 36*datalakeRootNodes.size() + indexAlgorithm.count*36;
        System.out.println("totalIndexByte3 = "+totalIndexByte3);
        System.out.println("silo:"+siloName+", tree index created time = "+(time2-time1));
//                indexDSS.storeDatalakeIndex(dataLakeRoot, "1", storeIndexPath);
        return (timee-time);

    }


    public Boolean isConnected(HashMap<Integer, HashSet<Integer>> query, HashMap<Integer, HashSet<Integer>> data, int[][] commonMBR){
        Boolean connected = false;
        int minX = (int) commonMBR[1][0];
        int minY = (int) commonMBR[1][1];
        int maxX = (int) commonMBR[0][0];
        int maxY = (int) commonMBR[0][1];
        if (connectedThreshold <= 1){
            for (int i = minX-1; i<=maxX; i++) {
                HashSet<Integer> query_hs;
                HashSet<Integer> data_hs;
                if (query.containsKey(i)){
                    query_hs = query.get(i);
                    if (data.containsKey(i)){
                        data_hs = data.get(i);
                        for (int j:query_hs){
                            if (j >= minY && j<= maxY){
                                if (data_hs.contains(j)|| data_hs.contains(j-1) || data_hs.contains(j+1)){
                                    connected = true;
                                    break;
                                }}
                        }
                    }if (data.containsKey(i+1)){
                        data_hs = data.get(i+1);
                        for (int j:query_hs){
                            if (j >= minY && j<= maxY){
                                if (data_hs.contains(j)|| data_hs.contains(j-1) || data_hs.contains(j+1)){
                                    connected = true;
                                    break;
                                }}
                        }
                    }
                }

                if (data.containsKey(i)){
                    data_hs = data.get(i);
                    if (query.containsKey(i)){
                        query_hs = query.get(i);
                        for (int j:data_hs){
                            if (j >= minY && j<= maxY){
                                if (query_hs.contains(j)|| query_hs.contains(j-1) || query_hs.contains(j+1)){
                                    connected = true;
                                    break;
                                }}
                        }
                    }if (query.containsKey(i+1)){
                        query_hs = query.get(i+1);
                        for (int j:data_hs){
                            if (j >= minY && j<= maxY){
                                if (query_hs.contains(j)|| query_hs.contains(j-1) || query_hs.contains(j+1)){
                                    connected = true;
                                    break;
                                }}
                        }
                    }
                }
            }
        }else {
            for (int i : query.keySet()) {
                HashSet<Integer> query_hs = query.get(i);
                HashSet<Integer> data_hs;
                for (int j: data.keySet()){
                    int x = j - i;
                    if (x <= connectedThreshold){
                        data_hs = data.get(i);
                        for (int t1: query_hs){
                            for (int t2: data_hs){
                                int y = t2 - t1;
                                if (t2-t1<connectedThreshold){
                                    double dist = Math.sqrt((x * x) + (y * y));
                                    if (dist <= connectedThreshold){
                                        connected = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }


        return connected;
    }

    public void MCPsubscribeQueryProcess(ArrayList<String> updateDatasetList, int queryID) throws CloneNotSupportedException{
        File file = new File(updateFilePath);
        boolean isUpdate = false;
//        PriorityQueue<relaxIndexNode> deviceResult = deviceResultList.get(queryId);
        HashSet originalDeviceResult = new HashSet();
        PriorityQueue<relaxIndexNode> updatedDeviceResult = new PriorityQueue<>(new ComparatorByRelaxIndexNodeReverse());
        int insertNum = 0;
        int findParent = 0;
        int nonUpdate = 0;
        for (relaxIndexNode re: deviceResult){
            originalDeviceResult.add(re.resultId);
        }

        if (file.exists()){
            for (int t = 0; t< updateDatasetList.size(); t++){
                String updateDatasetId = updateDatasetList.get(t);
                String dataPath = updateFilePath+updateDatasetId+".csv";
                double[][] MBR = new double[2][2];
                String datasetid = updateDatasetId;
                HashMap<Integer, HashSet<Integer>> modifiedXsliceInformation = new HashMap<>();
                HashSet<Long> hs = new HashSet<>();
                HashMap<Long, Double> zcodeHashMap = generateSignature(hs, MBR, minX, minY, rangeX, rangeY, resolution, dataPath);
                ArrayList<Long> arrayListModified = new ArrayList<>(hs);
                double max[], min[], pivot[], radius = 0;
                max = MBR[0];
                min = MBR[1];
                pivot = new double[dimension];
                for (int i = 0; i < arrayListModified.size(); i++) {
                    double[] datapoint = resolve(arrayListModified.get(i), resolution);
                    HashSet sliceHS = modifiedXsliceInformation.getOrDefault((int)datapoint[0], new HashSet<>());
                    sliceHS.add((int)datapoint[1]);
                    modifiedXsliceInformation.put((int)datapoint[0], sliceHS);
//                    for (int j = 0; j < dimension; j++) {
//                        pivot[j] += datapoint[j];
//                    }
                }

                for(int i=0; i<dimension;i++) {
                    radius += Math.pow((max[i] - min[i])/2, 2);
                }

                for (int i =0; i<dimension; i++){
                    pivot[i] = min[i]+max[i];
                }
                for (int i=0; i<dimension; i++){
                    pivot[i] = pivot[i]/2;
                }

                radius = Math.sqrt(radius);
                indexNode rootNode = new indexNode(dimension);
                rootNode.setMBRmax(max);
                rootNode.setMBRmin(min);
                rootNode.setRadius(radius);
                rootNode.setPivot(pivot);
                rootNode.setNodeid(datasetid);
                rootNode.setDeviceID(siloID);

                if (datalakeRootNodes.keySet().contains(datasetid)){
                    indexNode originalNode = datalakeRootNodes.get(datasetid);
                    zcodemap.put(datasetid, zcodeHashMap);
                    zcodeMBR.put(datasetid, MBR);
                    xSliceInformation.put(datasetid, modifiedXsliceInformation);
                    datasetIdMapSignature.put(datasetid, arrayListModified);
                    datalakeRootNodes.put(datasetid, rootNode);
                    if (isEqualMBR(MBR[0], MBR[1], originalNode.getMBRmax(), originalNode.getMBRmin())){
                        //修改后的数据集MBR未发生改变，无需处理； 但是相关数据集内部存储数据的信息进行修改。
                        nonUpdate++;
                    }else{
                        //修改后的数据集发生改变了，则在原有索引树中找到该数据集路径，然后记录其路径，修改上层节点的区域
                        // 从根节点dataLakeRoot开始检索，找到数据集位置然后反向进行更新 //反向查找
//                    indexNode modifiedIndexNode = indexNodesHM.get(datasetid);
                        indexAlgorithm.reverseFindParent(originalNode.getParentNode(), originalNode, dimension);
                        findParent++;
                    }
                }else{
                    zcodemap.put(datasetid, zcodeHashMap);
                    zcodeMBR.put(datasetid, MBR);
                    xSliceInformation.put(datasetid, modifiedXsliceInformation);
                    datasetIdMapSignature.put(datasetid, arrayListModified);
                    datalakeRootNodes.put(datasetid, rootNode);
                    insertNewDataset(rootNode, dataLakeRoot);
                    insertNum++;
                }

                //MCP
                double threshold;
                HashMap<Integer, HashSet<Integer>> newDataset = new HashMap<>();
                HashMap<Integer, HashSet<Integer>> newQuery = new HashMap<>();
                int[][] iMBR = new int[2][2];
                if (deviceResult.size()>=topK){
                    threshold = deviceResult.peek().getLb();
                    int intersectionUB =  sliceUpperBound(threshold, querySlice, modifiedXsliceInformation, datasetid, iMBR, newDataset, newQuery);
                    int upperbound = queryMapSignature.size() + arrayListModified.size();
                    int lowerbound = queryMapSignature.size() + arrayListModified.size() - intersectionUB;
                    if (upperbound <= threshold){
                        //prune
                    }else {
                        if (isConnected(newQuery, newDataset, iMBR)){
                            int newSetIntersection = sliceExactCompute(newQuery, newDataset, iMBR);
                            int maxSetCoverage = queryMapSignature.size() + arrayListModified.size()- newSetIntersection;
                            if (maxSetCoverage > threshold){
                                isUpdate = true;
                                deviceResult.poll();
                                relaxIndexNode in = new relaxIndexNode(datasetid, newSetIntersection);
                                deviceResult.add(in);
                            }
                        }

                    }

                }else {
                    double[][] commonMBR = new double[2][2];
                    double interNum = intersectedAreaMBR(rootNode, queryNode, commonMBR, dimension);
                    if (interNum > 0){
                        int[][] MBRR = new int[2][2];
                        MBRR[0][0] = (int)commonMBR[0][0];
                        MBRR[0][1] = (int)commonMBR[0][1];
                        MBRR[1][0] = (int)commonMBR[1][0];
                        MBRR[1][1] = (int)commonMBR[1][1];
                        if (isConnected(querySlice,modifiedXsliceInformation, MBRR)){
                            int setIntersection = sliceExactCompute(querySlice,modifiedXsliceInformation, MBRR);
                            relaxIndexNode in = new relaxIndexNode(datasetid, setIntersection);
                            deviceResult.add(in);
                        }
                    }

                }
            }
        }else {
            System.out.println("update directory isn't exist!");
            file.mkdir();
        }

        if (isUpdate == true){
            // Compared with the original and updated results, only the updated result is transmitted
            if (deviceResult.size() == originalDeviceResult.size()){
                while (!deviceResult.isEmpty()){
                    relaxIndexNode re = deviceResult.poll();
                    if (!originalDeviceResult.contains(re.resultId)){
                        updatedDeviceResult.add(re);
                    }
                }
            }

            String d_result = "";
            while (!updatedDeviceResult.isEmpty()){ // && count<topk
                relaxIndexNode r= updatedDeviceResult.poll();
                if (r.getLb() > 0.0){
                    d_result += r.resultId+","+r.getLb()+";"; //d.datasetIDList.get(d.histogram_name[r.resultId-1])是.csv文件名
                }
            }
//                System.out.println(silo.getSiloId() + ":" +d_result);
            String s = getSiloId() + ":" +d_result;
            byte[] b = s.getBytes();
            totalNumberOfBytes += (b.length*2);
            long t1 = System.currentTimeMillis();
            postRequest("http://localhost:8086/doTopKQuery", s); //没有记录字节数和传输时间
            long t2 = System.currentTimeMillis();
            totalNumberOfTransferTime += (t2-t1);

        }
    }

    public void MCPnonSubscribeQueryProcess(ArrayList<String> updateDatasetList) throws CloneNotSupportedException{
        File file = new File(updateFilePath);
        if (file.exists()){
            for (int t = 0; t< updateDatasetList.size(); t++){
                String updateDatasetId = updateDatasetList.get(t);
                String dataPath = updateFilePath+updateDatasetId+".csv";
//                System.out.println("update dataset path = "+dataPath);
                double[][] MBR = new double[2][2];
                String datasetid = updateDatasetId;
                HashMap<Integer, HashSet<Integer>> modifiedXsliceInformation = new HashMap<>();
                HashSet<Long> hs = new HashSet<>();
                HashMap<Long, Double> zcodeHashMap = generateSignature(hs, MBR, minX, minY, rangeX, rangeY, resolution, dataPath);
                ArrayList<Long> arrayListModified = new ArrayList<>(hs);
                double max[], min[], pivot[], radius = 0;
                max = MBR[0];
                min = MBR[1];
                pivot = new double[dimension];
                for (int i = 0; i < arrayListModified.size(); i++) {
                    double[] datapoint = resolve(arrayListModified.get(i), resolution);
                    HashSet sliceHS = modifiedXsliceInformation.getOrDefault((int)datapoint[0], new HashSet<>());
                    sliceHS.add((int)datapoint[1]);
                    modifiedXsliceInformation.put((int)datapoint[0], sliceHS);
                }
                for(int i=0; i<dimension;i++) {
                    radius += Math.pow((max[i] - min[i])/2, 2);
                }

                for (int i =0; i<dimension; i++){
                    pivot[i] = min[i]+max[i];
                }
                for (int i=0; i<dimension; i++){
                    pivot[i] = pivot[i]/2;
                }

                radius = Math.sqrt(radius);
                indexNode rootNode = new indexNode(dimension);
                rootNode.setMBRmax(max);
                rootNode.setMBRmin(min);
                rootNode.setRadius(radius);
                rootNode.setPivot(pivot);
                rootNode.setNodeid(datasetid);
                rootNode.setDeviceID(siloID);

                if (datalakeRootNodes.keySet().contains(datasetid)){
                    indexNode originalNode = datalakeRootNodes.get(datasetid);
                    zcodemap.put(datasetid, zcodeHashMap);
                    zcodeMBR.put(datasetid, MBR);
                    xSliceInformation.put(datasetid, modifiedXsliceInformation);
                    datasetIdMapSignature.put(datasetid, arrayListModified);
                    datalakeRootNodes.put(datasetid, rootNode);
                    if (isEqualMBR(MBR[0], MBR[1], originalNode.getMBRmax(), originalNode.getMBRmin())){
                    }else{
                        indexAlgorithm.reverseFindParent(originalNode.getParentNode(), originalNode, dimension);
                    }
                }else{
                    zcodemap.put(datasetid, zcodeHashMap);
                    zcodeMBR.put(datasetid, MBR);
                    xSliceInformation.put(datasetid, modifiedXsliceInformation);
                    datasetIdMapSignature.put(datasetid, arrayListModified);
                    datalakeRootNodes.put(datasetid, rootNode);
//                    insertNewDataset(rootNode, dataLakeRoot);
                }

            }
        }else {
            System.out.println("update directory isn't exist!");
            file.mkdir();
        }
        long time1 = System.currentTimeMillis();
        indexNode parentNode = new indexNode(2);
        dataLakeRoot = indexDSS.indexDatasetKD(datasetIdMapSignature, datalakeRootNodes, parentNode, dimension,capacity,1, siloID, false);
        long time2 = System.currentTimeMillis();

    }

    public void subscribeQueryProcess(ArrayList<String> updateDatasetList, int queryId) throws CloneNotSupportedException{
        File file = new File(updateFilePath);
//        PriorityQueue<relaxIndexNode> deviceResult = deviceResultList.get(queryId);
        boolean isUpdate = false;
        HashSet originalDeviceResult = new HashSet();
        PriorityQueue<relaxIndexNode> updatedDeviceResult = new PriorityQueue<>(new ComparatorByRelaxIndexNodeReverse());
        int insertNum = 0;
        int findParent = 0;
        int nonUpdate = 0;
        for (relaxIndexNode re: deviceResult){
            originalDeviceResult.add(re.resultId);
        }

        if (file.exists()){
            for (int t = 0; t< updateDatasetList.size(); t++){
                String updateDatasetId = updateDatasetList.get(t);
                String dataPath = updateFilePath+updateDatasetId+".csv";
                double[][] MBR = new double[2][2];
                String datasetid = updateDatasetId;
                HashMap<Integer, HashSet<Integer>> modifiedXsliceInformation = new HashMap<>();
                HashSet<Long> hs = new HashSet<>();
                HashMap<Long, Double> zcodeHashMap = generateSignature(hs, MBR, minX, minY, rangeX, rangeY, resolution, dataPath);

                ArrayList<Long> arrayListModified = new ArrayList<>(hs);
                double max[], min[], pivot[], radius = 0;
                max = MBR[0];
                min = MBR[1];
                pivot = new double[dimension];
                for (int i = 0; i < arrayListModified.size(); i++) {
                    double[] datapoint = resolve(arrayListModified.get(i), resolution);
                    HashSet sliceHS = modifiedXsliceInformation.getOrDefault((int)datapoint[0], new HashSet<>());
                    sliceHS.add((int)datapoint[1]);
                    modifiedXsliceInformation.put((int)datapoint[0], sliceHS);
//                    for (int j = 0; j < dimension; j++) {
//                        pivot[j] += datapoint[j];
//                    }
                }
                for(int i=0; i<dimension;i++) {
                    radius += Math.pow((max[i] - min[i])/2, 2);
                }

                for (int i =0; i<dimension; i++){
                    pivot[i] = min[i]+max[i];
                }
                for (int i=0; i<dimension; i++){
                    pivot[i] = pivot[i]/2;
                }
                radius = Math.sqrt(radius);
                indexNode rootNode = new indexNode(dimension);
                rootNode.setMBRmax(max);
                rootNode.setMBRmin(min);
                rootNode.setRadius(radius);
                rootNode.setPivot(pivot);
                rootNode.setNodeid(datasetid);
                rootNode.setDeviceID(siloID);

                if (datalakeRootNodes.keySet().contains(datasetid)){
                    indexNode originalNode = datalakeRootNodes.get(datasetid);
                    zcodemap.put(datasetid, zcodeHashMap);
                    zcodeMBR.put(datasetid, MBR);
                    xSliceInformation.put(datasetid, modifiedXsliceInformation);
                    datasetIdMapSignature.put(datasetid, arrayListModified);
                    datalakeRootNodes.put(datasetid, rootNode);
                    if (isEqualMBR(MBR[0], MBR[1], originalNode.getMBRmax(), originalNode.getMBRmin())){
                        nonUpdate++;
                    }else{
                        indexAlgorithm.reverseFindParent(originalNode.getParentNode(), originalNode, dimension);
                        findParent++;
                    }
                }else{
                    zcodemap.put(datasetid, zcodeHashMap);
                    zcodeMBR.put(datasetid, MBR);
                    xSliceInformation.put(datasetid, modifiedXsliceInformation);
                    datasetIdMapSignature.put(datasetid, arrayListModified);
                    datalakeRootNodes.put(datasetid, rootNode);
                    insertNewDataset(rootNode, dataLakeRoot);
                    insertNum++;
                }

                double threshold;
                ArrayList<Long> newDataset = new ArrayList<>();
                ArrayList<Long> newQuery = new ArrayList<>();
                if (deviceResult.size()>=topK){
                    threshold = deviceResult.peek().getLb();
                    int upperbound = upperBoundCompute(threshold,queryMapSignature,  arrayListModified, datasetid, newQuery,newDataset);
                    if (upperbound <= threshold){
                        //prune
                    }else {
                        int newSetIntersection = exactCompute(newQuery,newDataset);
                        if (newSetIntersection > threshold){
                            isUpdate = true;
                            deviceResult.poll();
                            relaxIndexNode in = new relaxIndexNode(datasetid, newSetIntersection);
                            deviceResult.add(in);
                        }
                    }

                }else {
                    int setIntersection = exactCompute(queryMapSignature,arrayListModified);
                    relaxIndexNode in = new relaxIndexNode(datasetid, setIntersection);
                    deviceResult.add(in);
                }
            }
        }else {
            System.out.println("update directory isn't exist!");
            file.mkdir();
        }

        if (isUpdate == true){
            //对比original和更新之后的，只传输更新之后的结果
            if (deviceResult.size() == originalDeviceResult.size()){
                while (!deviceResult.isEmpty()){
                    relaxIndexNode re = deviceResult.poll();
                    if (!originalDeviceResult.contains(re.resultId)){
                        updatedDeviceResult.add(re);
                    }
                }
            }

//            System.out.println("nonUpdate = "+nonUpdate+", insertNum ="+insertNum+", findParent  = "+ findParent);
            String d_result = "";
            while (!updatedDeviceResult.isEmpty()){ // && count<topk
                relaxIndexNode r= updatedDeviceResult.poll();
                if (r.getLb() > 0.0){
                    d_result += r.resultId+","+r.getLb()+";"; //d.datasetIDList.get(d.histogram_name[r.resultId-1])是.csv文件名
                }
            }
//                System.out.println(silo.getSiloId() + ":" +d_result);
            String s = getSiloId() + ":" +d_result;
            byte[] b = s.getBytes();
            totalNumberOfBytes += (b.length*2);
            long t1 = System.currentTimeMillis();
            postRequest("http://localhost:8086/doTopKQuery", s); //没有记录字节数和传输时间
            long t2 = System.currentTimeMillis();
            totalNumberOfTransferTime += (t2-t1);
        }
    }

    public void nonSubscribeQueryProcess(ArrayList<String> updateDatasetList) throws CloneNotSupportedException{
        File file = new File(updateFilePath);
        if (file.exists()){
            for (int t = 0; t< updateDatasetList.size(); t++){
                String updateDatasetId = updateDatasetList.get(t);
                String dataPath = updateFilePath+updateDatasetId+".csv";
//                System.out.println("update dataset path = "+dataPath);
                double[][] MBR = new double[2][2];
                String datasetid = updateDatasetId;
                HashMap<Integer, HashSet<Integer>> modifiedXsliceInformation = new HashMap<>();
                HashSet<Long> hs = new HashSet<>();
                HashMap<Long, Double> zcodeHashMap = generateSignature(hs, MBR, minX, minY, rangeX, rangeY, resolution, dataPath);
                ArrayList<Long> arrayListModified = new ArrayList<>(hs);
                double max[], min[], pivot[], radius = 0;
                max = MBR[0];
                min = MBR[1];
                pivot = new double[dimension];
                for (int i = 0; i < arrayListModified.size(); i++) {
                    double[] datapoint = resolve(arrayListModified.get(i), resolution);
                    HashSet sliceHS = modifiedXsliceInformation.getOrDefault((int)datapoint[0], new HashSet<>());
                    sliceHS.add((int)datapoint[1]);
                    modifiedXsliceInformation.put((int)datapoint[0], sliceHS);
//                    for (int j = 0; j < dimension; j++) {
//                        pivot[j] += datapoint[j];
//                    }
                }
                for(int i=0; i<dimension;i++) {
                    radius += Math.pow((max[i] - min[i])/2, 2);
                }

                for (int i =0; i<dimension; i++){
                    pivot[i] = min[i]+max[i];
                }
                for (int i=0; i<dimension; i++){
                    pivot[i] = pivot[i]/2;
                }
                radius = Math.sqrt(radius);
                indexNode rootNode = new indexNode(dimension);
                rootNode.setMBRmax(max);
                rootNode.setMBRmin(min);
                rootNode.setRadius(radius);
                rootNode.setPivot(pivot);
                rootNode.setNodeid(datasetid);
                rootNode.setDeviceID(siloID);

                if (datalakeRootNodes.keySet().contains(datasetid)){
                    indexNode originalNode = datalakeRootNodes.get(datasetid);
                    zcodemap.put(datasetid, zcodeHashMap);
                    zcodeMBR.put(datasetid, MBR);
                    xSliceInformation.put(datasetid, modifiedXsliceInformation);
                    datasetIdMapSignature.put(datasetid, arrayListModified);
                    datalakeRootNodes.put(datasetid, rootNode);
                    if (isEqualMBR(MBR[0], MBR[1], originalNode.getMBRmax(), originalNode.getMBRmin())){

                    }else{

                        indexAlgorithm.reverseFindParent(originalNode.getParentNode(), originalNode, dimension);
                    }
                }else{
                    zcodemap.put(datasetid, zcodeHashMap);
                    zcodeMBR.put(datasetid, MBR);
                    xSliceInformation.put(datasetid, modifiedXsliceInformation);
                    datasetIdMapSignature.put(datasetid, arrayListModified);
                    datalakeRootNodes.put(datasetid, rootNode);
//                    insertNewDataset(rootNode, dataLakeRoot);
                }

            }
        }else {
            System.out.println("update directory isn't exist!");
            file.mkdir();
        }

        long time1 = System.currentTimeMillis();
        indexNode parentNode = new indexNode(2);
        dataLakeRoot = indexDSS.indexDatasetKD(datasetIdMapSignature,datalakeRootNodes, parentNode, dimension,capacity,1, siloID, false);
        long time2 = System.currentTimeMillis();
    }


    public static TreeMap<Long,Double> generateUpdateSignature(TreeSet<Long> hs, double[][] MBR, double minX, double minY, double rangeX, double rangeY, int resolution, String Path) throws CloneNotSupportedException{
        double blockSizeX = rangeX/Math.pow(2,resolution);
        double blockSizeY = rangeY/Math.pow(2,resolution);
        TreeMap<Long,Double> ZorderDensityHist = new TreeMap<>();
        long t = (long)Math.pow(2,resolution);

        double[] maxMBR = {-1000000000, -1000000000};
        double[] minMBR = {Double.MAX_VALUE, Double.MAX_VALUE};
        try {
            String record = "";
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(Path)));
            reader.readLine();
            while((record=reader.readLine())!=null) {
                String[] fields = record.split(",");
                double x_coordinate = Double.parseDouble(fields[a]);
                double y_coordinate = Double.parseDouble(fields[b]);
                if (fields[a].matches("-?\\d+(\\.\\d+)?") && x_coordinate > -180 && x_coordinate < 180 &&
                        y_coordinate > -90 && y_coordinate < 90) {
                    double x = x_coordinate - minX;
                    double y = y_coordinate - minY;
                    long X = (long) (x / blockSizeX);  //行row
                    long Y = (long) (y / blockSizeY);  //列 col
                    long id = Y * t + X;
                    hs.add(id);
                    Double num = ZorderDensityHist.getOrDefault(id, new Double(0));
                    ZorderDensityHist.put(id, num + 1);
                    ZorderDensityHist.put(id, (double) (num + 1));
                    if (maxMBR[0] < X)
                        maxMBR[0] = X;
                    if (maxMBR[1] < Y)
                        maxMBR[1] = Y;
                    if (minMBR[0] > X)
                        minMBR[0] = X;
                    if (minMBR[1] > Y)
                        minMBR[1] = Y;
                }
            }
            MBR[0] = maxMBR.clone();
            MBR[1] = minMBR.clone();
            reader.close();
        }catch (IOException e) {
//            e.printStackTrace();
//            System.out.println("this dataset name isn't exist");
        }

        return ZorderDensityHist;
    }

    public boolean isEqualMBR(double[] maxMBR1, double[] minMBR1, double[] maxMBR2, double[] minMBR2 ){
        boolean flag = false;
        for (int dimension = 0; dimension < minMBR1.length; dimension++){
            if (minMBR1[dimension] == minMBR2[dimension] && maxMBR1[dimension] == maxMBR2[dimension]){
                flag = true;
            }
        }
        return flag;
    }


    // insert a new node into the index: finding the node that covers the ball most,
    // and insert to the leaf node, split if exceeding the maximum number of points,
    public indexNode insertNewDataset(indexNode newNode, indexNode root) {
        double minBox[] = new double[dimension];
        double maxBox[] = new double[dimension];
        double pivot[] = root.getPivot();
        double pivotn[] = newNode.getPivot();
        double newpivot[] = new double[dimension];

        int d=0;
        for(int dim=0; dim<dimension; dim++) {
            minBox[dim] = Math.min(pivot[dim]-root.getRadius(), pivotn[dim]-newNode.getRadius());
            maxBox[dim] = Math.max(pivot[dim]+root.getRadius(), pivotn[dim]+newNode.getRadius());
            newpivot[dim] = (maxBox[dim] + minBox[dim])/2;
        }
        root.setPivot(newpivot);//used the above techniques to get the new pivot and radius
        root.setRadius(distance(newpivot, minBox));

        //-1 denotes the leaf node
        if(root.getDatasetID()==-1) {//check whether it covers root nodes directly, the lowest level of the dataset tree
            root.addNodes(newNode);// refine the node by adding this node,
            if(root.getNodelist().size()>capacity) {//check whether we need to split,
                indexNode nodeleftIndexNode = new indexNode(dimension);
                indexNode noderightIndexNode = new indexNode(dimension);
                int counter = 0;
                for(indexNode aIndexNode: root.getNodelist()) {
                    if(aIndexNode.getPivot()[d] < pivot[d]) {// split based on the pivot point used above
                        nodeleftIndexNode.addNodes(aIndexNode);
                        counter++;
                    }else {
                        noderightIndexNode.addNodes(aIndexNode);
                    }
                }
                if(counter>0 && counter<root.getNodelist().size()) {
                    nodeleftIndexNode.setroot(-1);
                    noderightIndexNode.setroot(-1);
                    root.clearNodes();//clear the nodes
                    root.addNodes(nodeleftIndexNode);
                    root.addNodes(noderightIndexNode);
                    root.setroot(-2);
                }
            }
        }else if(root.getDatasetID()==-2){
            //-2 denotes the internal node
            indexNode tempIndexNode = null;
            double mindis = Double.MAX_VALUE;
            for(indexNode aIndexNode: root.getNodelist()) {// scan every node, and see which one is good, compute the
                double distance = distance(aIndexNode.getPivot(), newNode.getPivot());
                double coverDis = distance + aIndexNode.getRadius() + newNode.getRadius();
                if(coverDis < mindis) {
                    mindis = coverDis;
                    tempIndexNode = aIndexNode;
                }

            }

            root.removeNode(tempIndexNode);// we need to update this node, remove then add
            tempIndexNode = insertNewDataset(newNode, tempIndexNode);
            root.addNodes(tempIndexNode);
        }
        return root;
    }

    public void localIndexDatasetAdd(String path)throws CloneNotSupportedException, JsonProcessingException{
        File file = new File(path);
        File[] array = file.listFiles();
        if (file.exists()) {
            for (int t = 0; t < array.length; t++) {
//                System.out.println(array[t].getName());  //输出文件名
                String dataPath = array[t].getPath();
                double[][] MBR = new double[2][2];
                String datasetID = array[t].getName().split(".")[0];
                String datasetid = datasetID;
                //1, add
                HashMap<Integer, HashSet<Integer>> modifiedXsliceInformation = new HashMap<>();
                HashSet<Long> hs = new HashSet<>();
                HashMap<Long, Double> zcodeHashMap = generateSignature(hs, MBR, minX, minY, rangeX, rangeY, resolution, dataPath);
                ArrayList<Long> arrayListModified = new ArrayList<>(hs);
                double max[], min[], pivot[], radius = 0;
                max = MBR[0];
                min = MBR[1];
                pivot = new double[dimension];
                for (int i = 0; i < arrayListModified.size(); i++) {
                    double[] datapoint = resolve(arrayListModified.get(i), resolution);
                    HashSet sliceHS = modifiedXsliceInformation.getOrDefault((int)datapoint[0], new HashSet<>());
                    sliceHS.add((int)datapoint[1]);
                    modifiedXsliceInformation.put((int)datapoint[0], sliceHS);
//                    for (int j = 0; j < dimension; j++) {
//                        pivot[j] += datapoint[j];
//                    }
                }

                for(int i=0; i<dimension;i++) {
                    radius += Math.pow((max[i] - min[i])/2, 2);
                }

                for (int i =0; i<dimension; i++){
                    pivot[i] = min[i]+max[i];
                }
                for (int i=0; i<dimension; i++){
                    pivot[i] = pivot[i]/2;
                }
                radius = Math.sqrt(radius);
                indexNode rootNode = new indexNode(dimension);
                rootNode.setMBRmax(max);
                rootNode.setMBRmin(min);
                rootNode.setRadius(radius);
                rootNode.setPivot(pivot);
                rootNode.setNodeid(datasetid);
                rootNode.setDeviceID(siloID);
                datalakeRootNodes.put(datasetid, rootNode);
                //2, find the location to be inserted
                insertNewDataset(rootNode, dataLakeRoot);
            }
        }


    }

    public void localIndexDatasetDelete(ArrayList<Integer> arrayList){
        for (int i=0; i<arrayList.size(); i++){
            indexNode in = datalakeRootNodes.get(arrayList.get(i));
            indexNode in_Parent = in.getParentNode();
            in_Parent.deleteNodeIds(arrayList.get(i));
            indexNodesHM.remove(arrayList.get(i));
        }
    }

    public void localIndexDatasetUpdate(String path)throws CloneNotSupportedException{
        File file = new File(path);
        File[] array = file.listFiles();
        if (file.exists()){
            for (int t=0; t<array.length; t++){

                String dataPath = array[t].getPath();
                double[][] MBR = new double[2][2];

                String datasetID = array[t].getName().split(".")[0];
                String datasetid = datasetID;

                //1, add
                HashMap<Integer, HashSet<Integer>> modifiedXsliceInformation = new HashMap<>();
                HashSet<Long> hs = new HashSet<>();
                HashMap<Long, Double> zcodeHashMap = generateSignature(hs, MBR, minX, minY, rangeX, rangeY, resolution, dataPath);
                ArrayList<Long> arrayListModified = new ArrayList<>(hs);
                double max[], min[], pivot[], radius = 0;
                max = MBR[0];
                min = MBR[1];
                pivot = new double[dimension];
                for (int i = 0; i < arrayListModified.size(); i++) {
                    double[] datapoint = resolve(arrayListModified.get(i), resolution);
                    HashSet sliceHS = modifiedXsliceInformation.getOrDefault((int)datapoint[0], new HashSet<>());
                    sliceHS.add((int)datapoint[1]);
                    modifiedXsliceInformation.put((int)datapoint[0], sliceHS);

                }

                for(int i=0; i<dimension;i++) {
                    radius += Math.pow((max[i] - min[i])/2, 2);
                }

                for (int i =0; i<dimension; i++){
                    pivot[i] = min[i]+max[i];
                }
                for (int i=0; i<dimension; i++){
                    pivot[i] = pivot[i]/2;
                }

                radius = Math.sqrt(radius);
                indexNode rootNode = new indexNode(dimension);
                rootNode.setMBRmax(max);
                rootNode.setMBRmin(min);
                rootNode.setRadius(radius);
                rootNode.setPivot(pivot);
                rootNode.setNodeid(datasetid);
                rootNode.setDeviceID(siloID);

                datalakeRootNodes.put(datasetid, rootNode);

                //2,Find the dataset corresponding to the datasetID. If the new MBR is the same as the previous one, the data corresponding to the datasetID is not processed
                indexNode modified = datalakeRootNodes.get(Integer.parseInt(datasetID));
                if (isEqualMBR(MBR[0], MBR[1], modified.getMBRmax(), modified.getMBRmin())){
                    //The MBR of the modified data set has not changed and no processing is required.
                    zcodemap.put(datasetid, zcodeHashMap);
                    zcodeMBR.put(datasetid, MBR);
                    xSliceInformation.put(datasetid, modifiedXsliceInformation);
                    datasetIdMapSignature.put(datasetid, arrayListModified);

                }else{
                    indexNode modifiedIndexNode = indexNodesHM.get(datasetid);
                    reverseFindParent(modifiedIndexNode);
                }
            }
        }

    }

    public void reverseFindParent(indexNode modifiedIndexNode){
        indexNode parentIndexNode = modifiedIndexNode.getParentNode();
        //判断子节点的MBR是否包含在父节点里面
        double[] minMBRofParent = parentIndexNode.getMBRmin();
        double[] maxMBRofParent = parentIndexNode.getMBRmax();
        double[] minMBRofChild = modifiedIndexNode.getMBRmin();
        double[] maxMBRofChild = modifiedIndexNode.getMBRmax();

        for (int i = 0; i<dimension; i++){

            if (minMBRofChild[i] <= minMBRofParent[i] && maxMBRofChild[i] <= maxMBRofParent[i]){
            }else{
                double minBox[] = new double[dimension];
                double maxBox[] = new double[dimension];
                for(int dim = 0; dim < dimension; dim++){
                    if (minMBRofChild[dim] > minMBRofParent[dim]){
                        minBox[dim] = minMBRofParent[dim];
                    }else {
                        minBox[dim] = minMBRofChild[dim];
                    }
                    if (maxMBRofChild[dim] > maxMBRofParent[dim]){
                        maxBox[dim] = maxMBRofChild[dim];
                    }else{
                        maxBox[dim] = maxMBRofParent[dim];
                    }
                }
                parentIndexNode.setMBRmax(maxBox);
                parentIndexNode.setMBRmin(minBox);
                double radius = Math.max(distance2(parentIndexNode.getPivot(), minBox),distance2(parentIndexNode.getPivot(), maxBox));
                parentIndexNode.setRadius(radius);
                reverseFindParent(parentIndexNode);
            }
        }

    }


    public void Branch(ArrayList<indexNode> a, indexNode query){
        int[] sectNum = new int[a.size()];
        int Num = 0;
        for (int i = 0; i<a.size(); i++){
            indexNode ii = a.get(i);
            if (indexDSS.intersected(ii, query, dimension)){
                sectNum[i] = 1;
                Num = Num+1;
                System.out.print(ii.getNodeid()+",");
            }
        }
        System.out.println();
        System.out.println("set intersection number = "+Num);
    }



    public void sendLocalInedxUpdateInformationToServer(String url) throws IOException,CloneNotSupportedException {
        byte[] bytes = new byte[100];
        System.out.println("bytes.length = "+bytes.length+" byte");
        long time1 = System.currentTimeMillis();

        postRequest(url,"sendSingleUpdate:"+siloID+":"+ bytes.toString());//indexInfoWithoutCompressed.toString()
        long time2 = System.currentTimeMillis();
        System.out.println("transfer time = "+ (time2-time1)+"ms ");
    }

    public StringBuilder fileToString(String deviceID, File filePath) throws IOException {
        StringBuilder indexInfo = new StringBuilder();
        indexInfo.append(deviceID).append(";");
        try (FileInputStream fis = new FileInputStream(filePath);
             InputStreamReader isr = new InputStreamReader(fis,
                     StandardCharsets.UTF_8);
             BufferedReader br = new BufferedReader(isr)) {
            String line;
            while ((line = br.readLine()) != null) {
                indexInfo.append(line).append(";");
            }
        }
        return indexInfo;
    }

    public void postRequest(String url, String jsonStr) throws CloneNotSupportedException{
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost httpPost = new HttpPost(url);
        // jsonStr is the string to be sent
        long time1 = System.currentTimeMillis();
        StringEntity se = new StringEntity(jsonStr,"UTF-8");
        httpPost.setEntity(se);
        httpPost.setHeader("Content-Type", "application/json;charset=utf8");
        CloseableHttpResponse response = null;
        long time2 = System.currentTimeMillis();
//        System.out.println("transfer time = "+ (time2-time1)+"ms ");
        try {
            // Send Post request from client
            response = httpClient.execute(httpPost);
            // Get entity from response
            if (response.getEntity() != null) {
                String responseStr = EntityUtils.toString(response.getEntity());
                byte[] b = responseStr.getBytes();
                totalNumberOfBytes += (b.length *2);
                // begin top-k query
                if (jsonStr.equals("query:"+ siloID)){
//                    System.out.println("responseStr = "+responseStr);
                    getQueryFromResponse(responseStr);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                // Release resource
                if (httpClient != null) {
                    httpClient.close();
                }
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public String getSiloId() {
        return siloID;
    }

    public PriorityQueue<relaxIndexNode> index_STS3(ArrayList<Long> queryMapSignature,  int topK){
        HashMap<String, Integer> intersectionHm = new HashMap<>();
        long time1 = System.currentTimeMillis();
        for (int i =0; i<queryMapSignature.size(); i++){
            long t = queryMapSignature.get(i);
            if (postinglists.containsKey(t)) {
                HashSet<String> ILe = postinglists.get(t);
                for (String j: ILe) {
                    String key = j;
                    if (intersectionHm.containsKey(key)) {
                        int count = intersectionHm.get(key);
                        intersectionHm.put(key, count + 1);
                    } else {
                        intersectionHm.put(key, 1);
                    }
                }
            }
        }
        long time2 = System.currentTimeMillis();
//        System.out.println("traverse inverted index = "+(time2-time1)+" ms");
//        System.out.println("intersectionHm.size() = "+intersectionHm.size());
        PriorityQueue<relaxIndexNode> pq= new PriorityQueue<>(new ComparatorByRelaxIndexNodeReverse());  //small -> large
        for (String i: intersectionHm.keySet()){
            relaxIndexNode re = new relaxIndexNode(i, intersectionHm.get(i));
            pq.add(re);
        }
        while (pq.size() > topK){
            pq.poll();
        }
        return pq;
    }

    public PriorityQueue<relaxIndexNode> Pruning_STS3(ArrayList<Long> queryMapSignature,  int topK){
        PriorityQueue<relaxIndexNode> pq= new PriorityQueue<>(new ComparatorByRelaxIndexNodeReverse());  //small -> large

        return pq;
    }

    //unsorted
    public HashMap<String, HashMap<String, Integer>> geneOrloadGraph33(String graphfilePath) throws JacksonException, IOException {
        HashMap<String, HashMap<String, Integer>> graphInfo33 = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            File file = new File(graphfilePath);
            if (file.exists()) {
                FileInputStream inputStream = new FileInputStream(file);
                int length = inputStream.available();
                byte bytes[] = new byte[length];
                inputStream.read(bytes);
                inputStream.close();
                String jsonString =new String(bytes, StandardCharsets.UTF_8);
                HashMap<String, HashMap<String, Integer>> graphString = objectMapper.readValue(jsonString, HashMap.class);
                for (String s: graphString.keySet()){
                    HashMap<String, Integer> hm = graphString.get(s);
                    HashMap<String, Integer> hmInteger = new HashMap<String, Integer>();
                    for (String s1: hm.keySet()){
                        String key = s1;
                        int value = (int)hm.get(s1);
                        hmInteger.put(key , value);
                    }
                    graphInfo33.put(s, hmInteger);
                }

            } else {
                for (String i : datalakeRootNodes.keySet()) {
                    indexNode in = datalakeRootNodes.get(i);
                    ArrayList<Long> queryIn = datasetIdMapSignature.get(i);
                    HashMap<String, Integer> edgeInfo = new HashMap<>();
                    candidatesLeafnode.clear();
                    IBBranchAndBound(dataLakeRoot, in);
                    for (indexNode candi : candidatesLeafnode) {
                        TreeMap<Long, HashSet<String>> pl = candi.getPostinglist();
                        HashMap<String, Integer> tempResult = new HashMap();
                        for (long id : queryIn) {
                            if (pl.containsKey(id)) {
                                HashSet<String> posting = pl.get(id);
                                for (String p : posting) {
                                    if (!(p.equals(i))){
                                        Integer frequence = tempResult.getOrDefault(p, new Integer(0));
                                        frequence += 1;
                                        tempResult.put(p, frequence);
                                    }
                                }
                            }
                        }
                        for (String idd : tempResult.keySet()) {
                            edgeInfo.put(idd, tempResult.get(idd));
                        }
                    }
                    graphInfo33.put(i, edgeInfo);
                }
                String jsonString1 = objectMapper.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(graphInfo33);
                FileWriter fw = null;
                try {
                    fw = new FileWriter(graphfilePath);
                    fw.write(jsonString1);
                    fw.close();
                    System.out.println("graph write successfully");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch(Exception e){
            e.printStackTrace();
        }
        return graphInfo33;
    }


    //sorted
    public HashMap<String, LinkedHashMap<String,Integer>> graphInfoSorted = new HashMap<>();
    public void geneOrloadGraphSorted(String graphfilePath) throws JacksonException, IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            File file = new File(graphfilePath);
            if (file.exists()) {
                FileInputStream inputStream = new FileInputStream(file);
                int length = inputStream.available();
                byte bytes[] = new byte[length];
                inputStream.read(bytes);
                inputStream.close();
                String jsonString =new String(bytes, StandardCharsets.UTF_8);
                HashMap<String, LinkedHashMap<String, Integer>> graphString = objectMapper.readValue(jsonString, HashMap.class);
                for (String s: graphString.keySet()){
                    LinkedHashMap<String, Integer> hm = graphString.get(s);
                    LinkedHashMap<String, Integer> hmInteger = new LinkedHashMap<String, Integer>();
                    for (String s1: hm.keySet()){
                        String key = s1;
                        int value = (int)hm.get(s1);
                        hmInteger.put(key , value);
                    }
                    graphInfoSorted.put(s, hmInteger);
                }


            } else {
                for (String i : datalakeRootNodes.keySet()) {
                    indexNode in = datalakeRootNodes.get(i);
                    ArrayList<Long> queryIn = datasetIdMapSignature.get(i);
                    HashMap<String, Integer> edgeInfo = new HashMap<>();
                    candidatesLeafnode.clear();
                    IBBranchAndBound(dataLakeRoot, in);
                    //在索引树中遍历in，找到in相连接的节点，加入edgeInfo中
                    //1，如何判断是否连接
                    for (indexNode candi : candidatesLeafnode) {
                        //1,计算MBR;  //2，遍历query,找落在MBR中的点;  //3，找posting list中有没有该token;
                        TreeMap<Long, HashSet<String>> pl = candi.getPostinglist();
                        HashMap<String, Integer> tempResult = new HashMap();
                        for (long id : queryIn) {
                            if (pl.containsKey(id)) {
                                HashSet<String> posting = pl.get(id);
                                for (String p : posting) {
                                    if (!(p.equals(i))){
                                        Integer frequence = tempResult.getOrDefault(p, new Integer(0));
                                        frequence += 1;
                                        tempResult.put(p, frequence);
                                    }
                                }
                            }
                        }
                        for (String idd : tempResult.keySet()) {
                            edgeInfo.put(idd, tempResult.get(idd));
                        }
                    }
                    Comparator<Map.Entry<String, Integer>> valueComparator = new Comparator<Map.Entry<String, Integer>>() {
                        @Override
                        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                            return o2.getValue()-o1.getValue();
                        }
                    };
                    List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String,Integer>>(edgeInfo.entrySet());
                    Collections.sort(list,valueComparator);
                    LinkedHashMap<String, Integer> lhm = new LinkedHashMap<>();
                    for (Map.Entry<String, Integer> entry : list) {
                        lhm.put(entry.getKey(),entry.getValue());
                    }
                    graphInfoSorted.put(i, lhm);
                }
                String jsonString1 = objectMapper.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(graphInfoSorted);
                FileWriter fw = null;
                try {
                    fw = new FileWriter(graphfilePath);
                    fw.write(jsonString1);
                    fw.close();
                    System.out.println("graph write successfully");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    public HashMap<String, HashMap<String,Integer>> graphInfo = new HashMap<>();

    public HashMap<String, HashMap<String,Integer>> geneOrloadGraph(String graphfilePath) throws JacksonException, IOException {
        HashMap<String, HashMap<String,Integer>> graphInfoSer = new HashMap<>();
        try {
            File file = new File(graphfilePath);
            if (file.exists()) {
                System.out.println(graphfilePath + " is exist!!!");
                graphInfoSer = deSerializationZcurve(graphfilePath);
            } else {
                for (String i : datalakeRootNodes.keySet()) {
                    indexNode in = datalakeRootNodes.get(i);
                    ArrayList<Long> queryIn = datasetIdMapSignature.get(i);
                    HashMap<String, Integer> edgeInfo = new HashMap<>();
                    candidatesLeafnode.clear();
                    IBBranchAndBound(dataLakeRoot, in);
                    for (indexNode candi : candidatesLeafnode) {
                        TreeMap<Long, HashSet<String>> pl = candi.getPostinglist();
                        HashMap<String, Integer> tempResult = new HashMap();
                        for (long id : queryIn) {
                            if (pl.containsKey(id)) {
                                HashSet<String> posting = pl.get(id);
                                for (String p : posting) {
                                    if (!(p.equals(i))){
                                        Integer frequence = tempResult.getOrDefault(p, new Integer(0));
                                        frequence += 1;
                                        tempResult.put(p, frequence);
                                    }
                                }
                            }
                        }
                        for (String idd : tempResult.keySet()) {
                            edgeInfo.put(idd, tempResult.get(idd));
                        }
                    }
                    graphInfoSer.put(i, edgeInfo);
                }

                SerializedZcurve(graphfilePath, graphInfoSer);

            }
        } catch(Exception e){
            e.printStackTrace();
        }

        return graphInfoSer;
    }


//    public PriorityQueue<relaxIndexNode> greedyMcpGraph() throws JacksonException, IOException{
////        System.out.println("queryID = "+queryId);
//        PriorityQueue<relaxIndexNode> result = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
//            @Override
//            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
//                return (o1.getLb()-o2.getLb()>0)?1:-1;
//            }
//        }); //small - large
//        HashMap<String, Integer> edgeInfo = new HashMap<>();
////        System.out.println(graphInfo.containsKey(queryId));
//        if (graphInfo.containsKey(queryId)){
//            edgeInfo = graphInfo.get(queryId);
//        }else{
//            candidatesLeafnode.clear();
//            //贪心算法,迭代k次找到最优结
//            if (connectedThreshold==0){
//                IBBranchAndBound(dataLakeRoot, queryNode);
//            }else {
//                System.out.println("connectThreshold = "+ connectedThreshold);
//                IBBranchAndBoundMCP(dataLakeRoot, queryNode);
//            }
//
//            for (indexNode candi : candidatesLeafnode) {
//                //1,计算MBR;  //2，遍历query,找落在MBR中的点;  //3，找posting list中有没有该token;
//                TreeMap<Long, HashSet<String>> pl = candi.getPostinglist();
//                HashMap<String, Integer> tempResult = new HashMap<>();
//                for (long id : queryMapSignature) {
//                    for (long id2: pl.keySet()){
//                        if (distance(resolve(id, resolution), resolve(id2, resolution)) <= connectedThreshold) { //pl.containsKey(id) //如果pl中containsKey满足与id的距离约束，则
//                            HashSet<String> posting = pl.get(id);
//                            for (String p : posting) {
//                                if (!(p.equals(queryId))){
//                                    Integer frequence = tempResult.getOrDefault(p, new Integer(0));
//                                    frequence += 1;
//                                    tempResult.put(p, frequence);
//                                }
//                            }
//                        }
//                    }
//                }
////                System.out.println(queryId+": ");
//                for (String idd : tempResult.keySet()) {
//                    edgeInfo.put(idd, tempResult.get(idd));
////                    System.out.println(idd+", "+tempResult.get(idd));
//                }
//                graphInfo.put(queryId, edgeInfo); //edgeInfo也需要排序
//            }
//        }
//
//        HashMap<String,Integer> resultHs = new HashMap<>();
//        HashSet<String> resultlist = new HashSet<>();
//
//        resultHs.put(queryId, queryMapSignature.size());
//        resultlist.add(queryId);
//        int count = 1;
//        while (count <= topK) {
//            count++;
//            PriorityQueue<relaxIndexNode> hs = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
//                @Override
//                public int compare(relaxIndexNode o1, relaxIndexNode o2) {
//                    return (o2.getLb() - o1.getLb() > 0) ? 1 : -1;
//                }
//            });
//
//            for (String id : resultlist) {
//                double threshold = 0.0;
//                String bestDatasetId = "-1";
//                int exactCoverage = 0;
//                if (graphInfo.containsKey(id)) {
//                    edgeInfo = graphInfo.get(id);
////                    System.out.println("=========");
//                    for(String i: edgeInfo.keySet()){
//                        if (!resultlist.contains(i)){
//                            exactCoverage= datasetIdMapSignature.get(i).size()- edgeInfo.get(i);
////                            System.out.print("datasetIdMapSignature.get("+i+").size() = "+datasetIdMapSignature.get(i).size());
////                            System.out.println(",  edgeInfo.get("+i+") = "+edgeInfo.get(i)+ ", exactCoverage = "+exactCoverage);
//                            if(exactCoverage>threshold){
//                                bestDatasetId=i;
//                                threshold=exactCoverage;
//                            }
//                        }
//                    }
////                    System.out.println("=========");
//                    if (!bestDatasetId.equals("-1")) {
//                        hs.add(new relaxIndexNode(bestDatasetId, threshold));
//                    }
//                }
//            }
//            if (hs.size()>0){
//                relaxIndexNode re = hs.peek();
//                resultHs.put(re.resultId, (int)re.lb);
//                resultlist.add(re.resultId);
//            }
//
//        }
//
//        int value = queryMapSignature.size();
//        resultHs.remove(queryId);
//        for (String i :resultHs.keySet()){
////            System.out.println("i = "+i);
//            value += resultHs.get(i);
//            result.add(new relaxIndexNode(i, value));
////            System.out.println("i = "+i+", value = "+value);
//        }
//
////        deviceResult = result;
//        return result;
//
//    }

    public PriorityQueue<relaxIndexNode> greedyMcpGraph() throws JacksonException, IOException{
        PriorityQueue<relaxIndexNode> result = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
            @Override
            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                return (o1.getLb()-o2.getLb()>0)?1:-1;
            }
        }); //small - large
        HashMap<String, Integer> edgeInfo = new HashMap<>();
        //1,找到一个结果再加载graph
        //2,第一次加载之后就不再重复加载了
        candidatesLeafnode.clear();
        //贪心算法,迭代k次找到最优结
        if (connectedThreshold==0){
            System.out.println("connectThreshold = "+ connectedThreshold);
            IBBranchAndBound(dataLakeRoot, queryNode);
        }else {
            System.out.println("connectThreshold = "+ connectedThreshold);
            IBBranchAndBoundMCP(dataLakeRoot, queryNode);
        }
//        candidates.clear();
//        //贪心算法,迭代k次找到最优结
//        if (connectedThreshold == 0){
//            BranchAndBound(dataLakeRoot, queryNode);
//        }else {
//            BranchAndBoundMCP(dataLakeRoot, queryNode);
//        }
        mergeQuerySlice = (HashMap<Integer, HashSet<Integer>>) querySlice.clone();
        mergeQueryZorder =  new HashSet<Long>(queryMapSignature);
        mergeQueryIndexNode = queryNode;
        HashSet<String> hashSet = new HashSet<>();
        hashSet.add(queryId);
        System.out.println("candidatesLeafnode.size() = "+candidatesLeafnode.size());

        double threshold = 0.0;
        String bestDatasetId = "-1";
        HashMap<String, Integer> tempResult = new HashMap<>();

        for (indexNode candi : candidatesLeafnode){
            for (String candidateID : candi.getNodeIDList()) {
                if (!hashSet.contains(candidateID)) {
                    HashMap<Integer, HashSet<Integer>> dataSlice = xSliceInformation.get(candidateID);
                    ArrayList<Long> dataZorder = datasetIdMapSignature.get(candidateID);
                    //先找到MBR
                    double[][] commonMBR = new double[2][2];
                    double interNum = intersectedAreaMBR(datalakeRootNodes.get(candidateID), mergeQueryIndexNode, commonMBR, dimension);
                    if (distance(datalakeRootNodes.get(candidateID).getPivot(), mergeQueryIndexNode.getPivot()) -
                            datalakeRootNodes.get(candidateID).getRadius() - mergeQueryIndexNode.getRadius() <= connectedThreshold){
                        int[][] MBR = new int[2][2];
                        MBR[0][0] = (int) commonMBR[0][0];
                        MBR[0][1] = (int) commonMBR[0][1];
                        MBR[1][0] = (int) commonMBR[1][0];
                        MBR[1][1] = (int) commonMBR[1][1];
                        if (threshold == 0.0) {
                            if (isConnected(mergeQuerySlice, dataSlice, MBR)) {
                                int intersection = sliceExactCompute(mergeQuerySlice, dataSlice, MBR);
                                threshold = mergeQueryZorder.size() + dataZorder.size() - intersection;
                                bestDatasetId = candidateID;
                                tempResult.put(candidateID, intersection);
                            }
                        } else {
                            int upperBound = mergeQueryZorder.size() + dataZorder.size();
                            if (upperBound < threshold) {
                                //prune
                            } else {
                                // verify 验证是否需要修改threshold;
                                if (isConnected(mergeQuerySlice, dataSlice, MBR)) {
                                    int exactIntersection = sliceExactCompute(mergeQuerySlice, dataSlice, MBR);
                                    int exactCoverage = mergeQueryZorder.size() + dataZorder.size() - exactIntersection;
                                    if (exactCoverage > threshold) {
                                        bestDatasetId = candidateID;
                                        threshold = exactCoverage;
                                        tempResult.put(candidateID, exactIntersection);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }


        //找到最优的一个，然后加到edgeInfo里面
        //然后加载图
        //然后基于图进行后面的
        if (!bestDatasetId.equals("-1")){
            //加载
            long constructGraphTime = 0;
            if (dataSource.searchType.equals("MCP") && !dataSource.loadDatasetGraph){
                switch (dataSource.algo){
                    case "IBtree":
                        long time1 = System.currentTimeMillis();
                        if (dataSource.graphfilePath.endsWith("txt")){
                            graphInfo = geneOrloadGraph33(dataSource.graphfilePath);
                        }else {
                            graphInfo = geneOrloadGraph(dataSource.graphfilePath);
                        }
                        dataSource.loadDatasetGraph = true;
                        long time2 = System.currentTimeMillis();
                        constructGraphTime = (time2 - time1);
                        System.out.println("construct the graph = " + constructGraphTime+"ms");
                        break;
                }
            }

            for (String idd : tempResult.keySet()) {
                edgeInfo.put(idd, tempResult.get(idd));
            }
            graphInfo.put(queryId, edgeInfo); //edgeInfo也需要排序
            //加载完成之后

            edgeInfo = graphInfo.get(queryId);
            HashMap<String,Integer> resultHs = new HashMap<>();
            HashSet<String> resultlist = new HashSet<>();

            resultHs.put(queryId, queryMapSignature.size());
            resultlist.add(queryId);
            int count = 1;
            while (count <= topK) {
                count++;
                PriorityQueue<relaxIndexNode> hs = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
                    @Override
                    public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                        return (o2.getLb() - o1.getLb() > 0) ? 1 : -1;
                    }
                });
                for (String id : resultlist) {
                    threshold = 0.0;
                    bestDatasetId = "-1";
                    int exactCoverage = 0;
                    if (graphInfo.containsKey(id)) {
                        edgeInfo = graphInfo.get(id);
//                    System.out.println("=========");
                        for(String i: edgeInfo.keySet()){
                            if (!resultlist.contains(i)){
                                exactCoverage= datasetIdMapSignature.get(i).size()- edgeInfo.get(i);
//                            System.out.print("datasetIdMapSignature.get("+i+").size() = "+datasetIdMapSignature.get(i).size());
//                            System.out.println(",  edgeInfo.get("+i+") = "+edgeInfo.get(i)+ ", exactCoverage = "+exactCoverage);
                                if(exactCoverage>threshold){
                                    bestDatasetId=i;
                                    threshold=exactCoverage;
                                }
                            }
                        }
//                    System.out.println("=========");
                        if (!bestDatasetId.equals("-1")) {
                            hs.add(new relaxIndexNode(bestDatasetId, threshold));
                        }
                    }
                }
                if (hs.size()>0){
                    relaxIndexNode re = hs.peek();
                    resultHs.put(re.resultId, (int)re.lb);
                    resultlist.add(re.resultId);
                }

            }

            int value = queryMapSignature.size();
            resultHs.remove(queryId);
            for (String i :resultHs.keySet()){
//            System.out.println("i = "+i);
                value += resultHs.get(i);
                result.add(new relaxIndexNode(i, value));
//            System.out.println("i = "+i+", value = "+value);
            }

        }else{
            System.out.println(" no result");
        }
//        deviceResult = result;
        return result;

    }



    public PriorityQueue<relaxIndexNode> greedyMcp() throws CloneNotSupportedException{
        PriorityQueue<relaxIndexNode> result = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
            @Override
            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                return (o1.getLb()-o2.getLb()>0)?1:-1;
            }
        }); //small - large
        candidates.clear();
        //贪心算法,迭代k次找到最优结
        if (connectedThreshold == 0){
            BranchAndBound(dataLakeRoot, queryNode);
        }else {
            BranchAndBoundMCP(dataLakeRoot, queryNode);
        }

        //1, 初始化一个新的结果集合， new indexNode and new HashMap<Integer, HashSet<Integer>> mergeQuery
        mergeQuerySlice = (HashMap<Integer, HashSet<Integer>>) querySlice.clone();
        mergeQueryZorder =  new HashSet<Long>(queryMapSignature);
        mergeQueryIndexNode = queryNode;

        double threshold = 0.0;

        for(int i = 0; i<topK; i++){
            //2, Find the connected set with the largest coverage
            String bestDatasetId = "-1";
            for (String candidateID: candidates){
                HashMap<Integer, HashSet<Integer>> dataSlice = xSliceInformation.get(candidateID);
                ArrayList<Long> dataZorder = datasetIdMapSignature.get(candidateID);
                //find MBR
                double[][] commonMBR = new double[2][2];
                double interNum = intersectedAreaMBR(datalakeRootNodes.get(candidateID), mergeQueryIndexNode, commonMBR, dimension);
                if (distance(datalakeRootNodes.get(candidateID).getPivot(), mergeQueryIndexNode.getPivot()) -
                        datalakeRootNodes.get(candidateID).getRadius() - mergeQueryIndexNode.getRadius() <= connectedThreshold){
                    int[][] MBR = new int[2][2];
                    MBR[0][0] = (int) commonMBR[0][0];
                    MBR[0][1] = (int) commonMBR[0][1];
                    MBR[1][0] = (int) commonMBR[1][0];
                    MBR[1][1] = (int) commonMBR[1][1];
                    if (threshold == 0.0){
                        if (isConnected(mergeQuerySlice, dataSlice, MBR)){
                            int intersection = sliceExactCompute(mergeQuerySlice, dataSlice, MBR);
                            threshold = mergeQueryZorder.size()+ dataZorder.size() - intersection;
                            bestDatasetId = candidateID;
                        }
                    }else {
                        HashMap<Integer, HashSet<Integer>> newDataset = new HashMap<>();
                        HashMap<Integer, HashSet<Integer>> newQuery = new HashMap<>();
                        int upperboundIntersection = intersectionUpperBound(mergeQuerySlice, dataSlice, newDataset, newQuery, MBR);
                        int lowerBound = mergeQueryZorder.size()+ dataZorder.size() - upperboundIntersection;
                        int upperBound = mergeQueryZorder.size()+ dataZorder.size();
                        if (upperBound < threshold){
                            //prune
                        }if (lowerBound > threshold){
                            if (isConnected(mergeQuerySlice, dataSlice, MBR)){
                                bestDatasetId = candidateID;
                                int exactIntersection = MBRSliceExactCompute(newDataset, newQuery);
                                int exactCoverage = mergeQueryZorder.size()+ dataZorder.size() - exactIntersection;
                                threshold = exactCoverage;
                            }
                        }else {
                            // verify whether the threshold needs to be changed.
                            if (isConnected(mergeQuerySlice, dataSlice, MBR)){
                                int exactIntersection = MBRSliceExactCompute(newDataset, newQuery);
                                int exactCoverage = mergeQueryZorder.size()+ dataZorder.size() - exactIntersection;
                                if (exactCoverage > threshold){
                                    bestDatasetId = candidateID;
                                    threshold = exactCoverage;
                                }
                            }
                        }

                    }
                }

            }
            if (!bestDatasetId.equals("-1")){
                relaxIndexNode re = new relaxIndexNode(bestDatasetId, threshold);
                result.add(re);
                mergeQueryIndexNode = new indexNode(dimension);
                double[] mbrmin = new double[2];
                double[] mbrmax = new double[2];
//                System.out.println("bestDatasetId = "+bestDatasetId);
//                System.out.println(datalakeRootNodes.get(bestDatasetId).getMBRmin()[0]);
                mbrmin[0] = Math.min(mergeQueryIndexNode.getMBRmin()[0], datalakeRootNodes.get(bestDatasetId).getMBRmin()[0]);
                mbrmin[1] = Math.min(mergeQueryIndexNode.getMBRmin()[1], datalakeRootNodes.get(bestDatasetId).getMBRmin()[1]);
                mbrmax[0] = Math.max(mergeQueryIndexNode.getMBRmax()[0], datalakeRootNodes.get(bestDatasetId).getMBRmax()[0]);
                mbrmax[1] = Math.max(mergeQueryIndexNode.getMBRmax()[1], datalakeRootNodes.get(bestDatasetId).getMBRmax()[1]);
                mergeQueryIndexNode.setMBRmin(mbrmin);
                mergeQueryIndexNode.setMBRmax(mbrmax);
                mergeQueryIndexNode.addNodeIds(bestDatasetId);
                ArrayList<Long> bestDataZorder = datasetIdMapSignature.get(bestDatasetId);

                for (int t = 0; t<bestDataZorder.size(); t++){
                    mergeQueryZorder.add(bestDataZorder.get(t));
                }

                HashMap<Integer, HashSet<Integer>> bestDataSlice = xSliceInformation.get(bestDatasetId);
                for (int u: bestDataSlice.keySet()){
                    if (mergeQuerySlice.containsKey(u)){
                        for (int v: bestDataSlice.get(u)){
                            mergeQuerySlice.get(u).add(v);
                        }
                    }else {
                        mergeQuerySlice.put(u, bestDataSlice.get(u));
                    }
                }
//            mergeQuerySlice;

                //2，放入结果集合中合并，然后i+1进行新一轮的搜索
                if (i < topK-1){
                    candidates.clear();
                    //重新搜索一遍找到candidates
                    if (connectedThreshold == 0){
                        BranchAndBound(dataLakeRoot, mergeQueryIndexNode);
                    }else {
                        BranchAndBoundMCP(dataLakeRoot, mergeQueryIndexNode);
                    }
                }
            }else{
//                System.out.println("no connected datasets");
                break;
            }

        }
//        deviceResult = result;
        return result;
    }


    public PriorityQueue<relaxIndexNode> greedyMcpOnlytree() throws CloneNotSupportedException{
        PriorityQueue<relaxIndexNode> result = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
            @Override
            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                return (o1.getLb()-o2.getLb()>0)?1:-1;
            }
        }); //small - large
        candidates.clear();
        //贪心算法,迭代k次找到最优结
        if (connectedThreshold == 0){
            BranchAndBound(dataLakeRoot, queryNode);
        }else {
            BranchAndBoundMCP(dataLakeRoot, queryNode);
        }

        //1, 初始化一个新的结果集合， new indexNode and new HashMap<Integer, HashSet<Integer>> mergeQuery
        mergeQuerySlice = (HashMap<Integer, HashSet<Integer>>) querySlice.clone();
        mergeQueryZorder =  new HashSet<Long>(queryMapSignature);
        mergeQueryIndexNode = queryNode;
        HashSet<String> hs = new HashSet<>();
        hs.add(queryId);

        double threshold = 0.0;
        for(int i = 0; i<=topK; i++) {
            //2, 找到相连接的且并集最大的集合
            String bestDatasetId = "-1";
            for (String candidateID : candidates) {
                if (!hs.contains(candidateID)) {
                    HashMap<Integer, HashSet<Integer>> dataSlice = xSliceInformation.get(candidateID);
                    ArrayList<Long> dataZorder = datasetIdMapSignature.get(candidateID);
                    //先找到MBR
                    double[][] commonMBR = new double[2][2];
                    double interNum = intersectedAreaMBR(datalakeRootNodes.get(candidateID), mergeQueryIndexNode, commonMBR, dimension);
                    if (distance(datalakeRootNodes.get(candidateID).getPivot(), mergeQueryIndexNode.getPivot()) -
                            datalakeRootNodes.get(candidateID).getRadius() - mergeQueryIndexNode.getRadius() <= connectedThreshold){
                        int[][] MBR = new int[2][2];
                        MBR[0][0] = (int) commonMBR[0][0];
                        MBR[0][1] = (int) commonMBR[0][1];
                        MBR[1][0] = (int) commonMBR[1][0];
                        MBR[1][1] = (int) commonMBR[1][1];
                        if (threshold == 0.0) {
                            if (isConnected(mergeQuerySlice, dataSlice, MBR)) {
                                int intersection = sliceExactCompute(mergeQuerySlice, dataSlice, MBR);
//                            System.out.print("mergeQueryZorder.size() = " + mergeQueryZorder.size() + ", " + candidateID + ", " + dataZorder.size() + ", " + intersection);
                                threshold = mergeQueryZorder.size() + dataZorder.size() - intersection;
                                bestDatasetId = candidateID;
//                            System.out.println(",  threshold = " + threshold);
                            }
                        } else {
                            int upperBound = mergeQueryZorder.size() + dataZorder.size();
                            if (upperBound < threshold) {
                                //prune
                            } else {
                                // verify 验证是否需要修改threshold;
                                if (isConnected(mergeQuerySlice, dataSlice, MBR)) {
                                    int exactIntersection = sliceExactCompute(mergeQuerySlice, dataSlice, MBR);
                                    int exactCoverage = mergeQueryZorder.size() + dataZorder.size() - exactIntersection;
                                    if (exactCoverage > threshold) {
                                        bestDatasetId = candidateID;
                                        threshold = exactCoverage;
                                    }
//                                System.out.println(mergeQueryZorder.size() + ", " + candidateID + ", " + dataZorder.size() + ", " + exactCoverage);

                                }
                            }
                        }
                    }
                }
            }

            if (!bestDatasetId.equals("-1")){
                relaxIndexNode re = new relaxIndexNode(bestDatasetId, threshold);
                result.add(re);
                mergeQueryIndexNode = new indexNode(dimension);
                double[] mbrmin = new double[2];
                double[] mbrmax = new double[2];
//                System.out.println("bestDatasetId = "+bestDatasetId);
//                System.out.println(datalakeRootNodes.get(bestDatasetId).getMBRmin()[0]);
                mbrmin[0] = Math.min(mergeQueryIndexNode.getMBRmin()[0], datalakeRootNodes.get(bestDatasetId).getMBRmin()[0]);
                mbrmin[1] = Math.min(mergeQueryIndexNode.getMBRmin()[1], datalakeRootNodes.get(bestDatasetId).getMBRmin()[1]);
                mbrmax[0] = Math.max(mergeQueryIndexNode.getMBRmax()[0], datalakeRootNodes.get(bestDatasetId).getMBRmax()[0]);
                mbrmax[1] = Math.max(mergeQueryIndexNode.getMBRmax()[1], datalakeRootNodes.get(bestDatasetId).getMBRmax()[1]);
                mergeQueryIndexNode.setMBRmin(mbrmin);
                mergeQueryIndexNode.setMBRmax(mbrmax);
                mergeQueryIndexNode.addNodeIds(bestDatasetId);
                ArrayList<Long> bestDataZorder = datasetIdMapSignature.get(bestDatasetId);
                for (int t = 0; t<bestDataZorder.size(); t++){
                    mergeQueryZorder.add(bestDataZorder.get(t));
                }
                HashMap<Integer, HashSet<Integer>> bestDataSlice = xSliceInformation.get(bestDatasetId);
                for (int u: bestDataSlice.keySet()){
                    if (mergeQuerySlice.containsKey(u)){
                        for (int v: bestDataSlice.get(u)){
                            mergeQuerySlice.get(u).add(v);
                        }
                    }else {
                        mergeQuerySlice.put(u, bestDataSlice.get(u));
                    }
                }
                //2, Merge into the result set, and then conduct the next search
                if (i < topK-1){
                    candidates.clear();
                    if (connectedThreshold == 0){
                        BranchAndBound(dataLakeRoot, mergeQueryIndexNode);
                    }else {
                        BranchAndBoundMCP(dataLakeRoot, mergeQueryIndexNode);
                    }

                }
            }else{
//                System.out.println("no connected datasets");
                break;
            }

        }
//        deviceResult = result;
        return result;
    }

//    public PriorityQueue<relaxIndexNode> invertedIndexMcp() throws CloneNotSupportedException{
//        PriorityQueue<relaxIndexNode> result = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
//            @Override
//            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
//                return (o1.getLb()-o2.getLb()>0)?1:-1;
//            }
//        }); //small - large
//        HashMap<String, Integer> candidateHm;
//        mergeQueryZorder =  new HashSet<Long>(queryMapSignature);
//        //贪心算法,迭代k次找到最优结
//        double threshold = 0.0;
//        for(int i = 0; i<topK; i++) {
//            //2, 找到相连接的且并集最大的集合
//            //利用倒排索引找到候选集
//            candidateHm = new HashMap<>();
//            for (long l : mergeQueryZorder) {
////
//                if (postinglists.containsKey(l)) {
//                    HashSet<String> candiDatasetID = postinglists.get(l);
//                    for (String j : candiDatasetID) {
//                        String datasetId = j;
//                        Integer frequence = candidateHm.getOrDefault(datasetId, new Integer(0));
//                        frequence += 1;
//                        candidateHm.put(datasetId, frequence);
//                    }
//                }
//                long r = (long) Math.pow(2, resolution);
//                long[] connectedID = new long[]{l + 1, l - 1, l + r, l - r, l + r + 1, l + r - 1, l - r + 1, l - r - 1}; //上下左右对角，8个，注意 frequency = 0，文中要解释一下对比算法是怎么工作的
//                //for循环
//                for (int t = 0; t < connectedID.length; t++) {
//
//                }
//                if (postinglists.containsKey(l)) {
//                    HashSet<String> candiDatasetID = postinglists.get(l);
//                    for (String j : candiDatasetID) {
//                        String datasetId = j;
//                        Integer frequence = candidateHm.getOrDefault(datasetId, new Integer(0));
//                        frequence += 1;
//                        candidateHm.put(datasetId, frequence);
//                    }
//                }
//
//
//            }
//            String bestDatasetId = "-1";
//            for (String candidateID : datasetIdMapSignature.keySet()) {
//                //1, whether the query is connected to each candidate
//                ArrayList<Long> dataZorder = datasetIdMapSignature.get(candidateID);
//
//                if (isConnected(mergeQueryZorder, new HashSet<Long>(dataZorder))) {
//                    int intersection, exactCoverage;
//                    if (candidateHm.keySet().contains(candidateID)) {
//                        intersection = candidateHm.get(candidateID);
//                        exactCoverage = mergeQueryZorder.size() + dataZorder.size() - intersection;
//                    } else {
//                        intersection = 0;
//                        exactCoverage = mergeQueryZorder.size() + dataZorder.size() - intersection;
//                    }
//
//                    if (exactCoverage > threshold) {
//                        bestDatasetId = candidateID;
//                        threshold = exactCoverage;
//                    }
//                }
//            }
//            if (!bestDatasetId.equals("-1")){
//
//                relaxIndexNode re = new relaxIndexNode(bestDatasetId, threshold);
//                result.add(re);
//                ArrayList<Long> bestDataZorder = datasetIdMapSignature.get(bestDatasetId);
//                for (int t = 0; t < bestDataZorder.size(); t++) {
//                    mergeQueryZorder.add(bestDataZorder.get(t));
//                }
//
//                //2，放入结果集合中合并，然后i+1进行新一轮的搜索
//                if (i < topK - 1) {
//                    candidates.clear();
//                    //重新搜索一遍找到candidates
//                    candidateHm = new HashMap<>();
//                    for (long l : mergeQueryZorder) {
//                        if (postinglists.containsKey(l)) {
//                            HashSet<String> candiDatasetID = postinglists.get(l);
//                            for (String j : candiDatasetID) {
//                                String datasetId = j;
//                                Integer frequence = candidateHm.getOrDefault(datasetId, new Integer(0));
//                                frequence += 1;
//                                candidateHm.put(datasetId, frequence);
//                            }
//                        }
//                    }
//                }
//            }else {
////                    System.out.println("no Connected datasets");
//                break;
//            }
//
//        }
////        deviceResult = result;
//        return result;
//    }

    public PriorityQueue<relaxIndexNode> standardGreedyMcpWithoutIndex() throws CloneNotSupportedException{
        PriorityQueue<relaxIndexNode> result = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
            @Override
            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                return (o1.getLb()-o2.getLb()>0)?1:-1;
            }
        }); //small - large

        //贪心算法,迭代k次找到最优结
        //1, 初始化一个新的结果集合，
        mergeQuerySlice = (HashMap<Integer, HashSet<Integer>>) querySlice.clone();
        mergeQueryZorder =  new HashSet<Long>(queryMapSignature);
        mergeQueryIndexNode = queryNode;
        double threshold = 0.0;
        for(int i = 0; i<topK; i++){
            //2, 找到相连接的且并集最大的集合
            String bestDatasetId = "-1";
            for (String candidateID: datasetIdMapSignature.keySet()){
                HashMap<Integer, HashSet<Integer>> dataSlice = xSliceInformation.get(candidateID);
                ArrayList<Long> dataZorder = datasetIdMapSignature.get(candidateID);
                //先找到MBR
                double[][] commonMBR = new double[2][2];
                double interNum = intersectedAreaMBR(datalakeRootNodes.get(candidateID), mergeQueryIndexNode, commonMBR, dimension);
                if (distance(datalakeRootNodes.get(candidateID).getPivot(), mergeQueryIndexNode.getPivot()) -
                        datalakeRootNodes.get(candidateID).getRadius() - mergeQueryIndexNode.getRadius() <= connectedThreshold){
                    int[][] MBR = new int[2][2];
                    MBR[0][0] = (int) commonMBR[0][0];
                    MBR[0][1] = (int) commonMBR[0][1];
                    MBR[1][0] = (int) commonMBR[1][0];
                    MBR[1][1] = (int) commonMBR[1][1];
                    if (isConnected(mergeQuerySlice, dataSlice, MBR)){
                        int setIntersection = 0;
                        for (long l_query : mergeQueryZorder) {
                            for (long l_dataset : dataZorder) {
                                if (l_query == l_dataset) {
                                    setIntersection++;
                                }
                            }
                        }
                        int value = mergeQueryZorder.size()+ dataZorder.size() - setIntersection;
//                            int intersection = sliceExactCompute(mergeQuerySlice, dataSlice, MBR);
//                            threshold = mergeQueryZorder.size()+ dataZorder.size() - intersection;
                        if (threshold < value ){
                            threshold = value;
                            bestDatasetId = candidateID;
                        }
                    }
                }
            }
            if (!bestDatasetId.equals("-1")){
                //2，放入结果集合中合并，然后i+1进行新一轮的搜索
                relaxIndexNode re = new relaxIndexNode(bestDatasetId, threshold);
                result.add(re);
                mergeQueryIndexNode = new indexNode(dimension);
                double[] mbrmin = new double[2];
                double[] mbrmax = new double[2];
                mbrmin[0] = Math.min(mergeQueryIndexNode.getMBRmin()[0], datalakeRootNodes.get(bestDatasetId).getMBRmin()[0]);
                mbrmin[1] = Math.min(mergeQueryIndexNode.getMBRmin()[1], datalakeRootNodes.get(bestDatasetId).getMBRmin()[1]);
                mbrmax[0] = Math.max(mergeQueryIndexNode.getMBRmax()[0], datalakeRootNodes.get(bestDatasetId).getMBRmax()[0]);
                mbrmax[1] = Math.max(mergeQueryIndexNode.getMBRmax()[1], datalakeRootNodes.get(bestDatasetId).getMBRmax()[1]);
                mergeQueryIndexNode.setMBRmin(mbrmin);
                mergeQueryIndexNode.setMBRmax(mbrmax);
                mergeQueryIndexNode.addNodeIds(bestDatasetId);
                ArrayList<Long> bestDataZorder = datasetIdMapSignature.get(bestDatasetId);
                for (int t = 0; t<bestDataZorder.size(); t++){
                    mergeQueryZorder.add(bestDataZorder.get(t));
                }
                HashMap<Integer, HashSet<Integer>> bestDataSlice = xSliceInformation.get(bestDatasetId);
                for (int u: bestDataSlice.keySet()){
                    if (mergeQuerySlice.containsKey(u)){
                        for (int v: bestDataSlice.get(u)){
                            mergeQuerySlice.get(u).add(v);
                        }
                    }else {
                        mergeQuerySlice.put(u, bestDataSlice.get(u));
                    }
                }
            }else {
//            System.out.println("no Connected datasets");
                break;
            }
        }
//        deviceResult = result;
        return result;
    }

    public PriorityQueue<relaxIndexNode> standardGreedyMcp() throws CloneNotSupportedException{
        PriorityQueue<relaxIndexNode> result = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
            @Override
            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                return (o1.getLb()-o2.getLb()>0)?1:-1;
            }
        }); //small - large

        candidates.clear();
        // Greedy algorithm, iteration k times to find the optimal node
        if (connectedThreshold == 0){
            BranchAndBound(dataLakeRoot, queryNode);
        }else {
            BranchAndBoundMCP(dataLakeRoot, queryNode);
        }

        //1,Initializes new indexNode and new HashMap<Integer, HashSet<Integer>> mergeQuery
        mergeQuerySlice = (HashMap<Integer, HashSet<Integer>>) querySlice.clone();
        mergeQueryZorder =  new HashSet<Long>(queryMapSignature);
        mergeQueryIndexNode = queryNode;
        double threshold = 0.0;
        for(int i = 0; i<topK; i++){
            String bestDatasetId = "-1";
            for (String candidateID: candidates){
                HashMap<Integer, HashSet<Integer>> dataSlice = xSliceInformation.get(candidateID);
                ArrayList<Long> dataZorder = datasetIdMapSignature.get(candidateID);
                double[][] commonMBR = new double[2][2];
                double interNum = intersectedAreaMBR(datalakeRootNodes.get(candidateID), mergeQueryIndexNode, commonMBR, dimension);
                if (distance(datalakeRootNodes.get(candidateID).getPivot(), mergeQueryIndexNode.getPivot()) -
                        datalakeRootNodes.get(candidateID).getRadius() - mergeQueryIndexNode.getRadius() <= connectedThreshold){
                    int[][] MBR = new int[2][2];
                    MBR[0][0] = (int) commonMBR[0][0];
                    MBR[0][1] = (int) commonMBR[0][1];
                    MBR[1][0] = (int) commonMBR[1][0];
                    MBR[1][1] = (int) commonMBR[1][1];
                    if (isConnected(mergeQuerySlice, dataSlice, MBR)){
                        int intersection = sliceExactCompute(mergeQuerySlice, dataSlice, MBR);
                        int exactCoverage = mergeQueryZorder.size()+ dataZorder.size() - intersection;
                        if (exactCoverage > threshold){
                            bestDatasetId = candidateID;
                            threshold = exactCoverage;
                        }
                    }
                }

            }
            relaxIndexNode re = new relaxIndexNode(bestDatasetId, threshold);
            result.add(re);
            mergeQueryIndexNode = new indexNode(dimension);
            double[] mbrmin = new double[2];
            double[] mbrmax = new double[2];
            mbrmin[0] = Math.min(mergeQueryIndexNode.getMBRmin()[0], datalakeRootNodes.get(bestDatasetId).getMBRmin()[0]);
            mbrmin[1] = Math.min(mergeQueryIndexNode.getMBRmin()[1], datalakeRootNodes.get(bestDatasetId).getMBRmin()[1]);
            mbrmax[0] = Math.max(mergeQueryIndexNode.getMBRmax()[0], datalakeRootNodes.get(bestDatasetId).getMBRmax()[0]);
            mbrmax[1] = Math.max(mergeQueryIndexNode.getMBRmax()[1], datalakeRootNodes.get(bestDatasetId).getMBRmax()[1]);
            mergeQueryIndexNode.setMBRmin(mbrmin);
            mergeQueryIndexNode.setMBRmax(mbrmax);
            mergeQueryIndexNode.addNodeIds(bestDatasetId);
            ArrayList<Long> bestDataZorder = datasetIdMapSignature.get(bestDatasetId);
            for (int t = 0; t<bestDataZorder.size(); t++){
                mergeQueryZorder.add(bestDataZorder.get(t));
            }
            HashMap<Integer, HashSet<Integer>> bestDataSlice = xSliceInformation.get(bestDatasetId);
            for (int u: bestDataSlice.keySet()){
                if (mergeQuerySlice.containsKey(u)){
                    for (int v: bestDataSlice.get(u)){
                        mergeQuerySlice.get(u).add(v);
                    }
                }else {
                    mergeQuerySlice.put(u, bestDataSlice.get(u));
                }
            }

            if (i < topK-1){
                candidates.clear();
                //重新搜索一遍找到candidates
                if (connectedThreshold == 0){
                    BranchAndBound(dataLakeRoot, mergeQueryIndexNode);
                }else {
                    BranchAndBoundMCP(dataLakeRoot, mergeQueryIndexNode);
                }


            }
        }
//        deviceResult = result;
        return result;
    }

    public int MBRSliceExactCompute(HashMap<Integer, HashSet<Integer>> newDataset, HashMap<Integer, HashSet<Integer>> newQuery){
        int newSetIntersection = 0;
        for (int i: newQuery.keySet()){
            HashSet<Integer> s_data = newDataset.get(i);
            HashSet<Integer> s_query = newQuery.get(i);
            if (s_data.size()>0 && s_query.size()>0) {
                for (int j : s_query) {
                    for (int z : s_data) {
                        if (j == z) {
                            newSetIntersection++;
                        }
                    }
                }
            }
        }
        return newSetIntersection;
    }

    public int sliceExactCompute(HashMap<Integer, HashSet<Integer>> query, HashMap<Integer, HashSet<Integer>> data, int[][] MBR){
        int intersection = 0;
        int minX = MBR[1][0];
        int minY = MBR[1][1];
        int maxX = MBR[0][0];
        int maxY = MBR[0][1];
        for (int i = minX; i<= maxX; i++){
            if (query.containsKey(i) && data.containsKey(i)){
                HashSet<Integer> hs_query = query.get(i);
                HashSet<Integer> hs_data = data.get(i);
                for (int hs_id: hs_data){
                    if (hs_query.contains(hs_id)){
                        intersection++;
                    }
                }
            }
        }
        return intersection;
    }

    public  int intersectionUpperBound(HashMap<Integer, HashSet<Integer>> querySlice, HashMap<Integer, HashSet<Integer>> dataSlice,
                                       HashMap<Integer, HashSet<Integer>> newDataset, HashMap<Integer, HashSet<Integer>> newQuery, int[][] commonMBR){

        int upperbound = 0;
        int dataNumFallMBR = 0;
        int queryNumFallMBR = 0;
        int minX = commonMBR[1][0];
        int minY = commonMBR[1][1];
        int maxX = commonMBR[0][0];
        int maxY = commonMBR[0][1];
        for (int i = minX; i<=maxX; i++){
            if (querySlice.containsKey(i) && dataSlice.containsKey(i)){
                HashSet<Integer> hs_query = querySlice.get(i);
                HashSet<Integer> new_hs_query = new HashSet<>();
                for (int hs_id: hs_query){
                    if (hs_id >= minY && hs_id <= maxY){
                        queryNumFallMBR++;
                        new_hs_query.add(hs_id);
                    }
                }

                HashSet<Integer> hs_data = dataSlice.get(i);
                HashSet<Integer> new_hs_data = new HashSet<>();
                for (int hs_id: hs_data){
                    if (hs_id >= minY && hs_id <= maxY){
                        dataNumFallMBR++;
                        new_hs_data.add(hs_id);
                    }
                }
                newDataset.put(i, new_hs_data);
                newQuery.put(i, new_hs_query);

            }
        }
        upperbound = Math.min(dataNumFallMBR, queryNumFallMBR);

        return upperbound;
    }

    public PriorityQueue<relaxIndexNode> topKQuery(String queryId) throws CloneNotSupportedException{
        PriorityQueue<relaxIndexNode> deviceResult = new PriorityQueue<>(new ComparatorByRelaxIndexNodeReverse());
        System.out.println("algorithm = "+algorithm);
        switch(algorithm)
        {
            case "STS3":
                long time01 = System.currentTimeMillis();
                postinglists = createPostinglists(datasetIdMapSignature);
                long totalIndexByte = 0;
                totalIndexByte += 8*postinglists.size();
                for (long i: postinglists.keySet()){
                    HashSet hs = postinglists.get(i);
                    totalIndexByte += 4*hs.size();
                }
                System.out.println("totalIndexByte = "+ totalIndexByte);
                long time02 = System.currentTimeMillis();
                System.out.println("silo:"+siloName+", posting list created time = "+(time02-time01)+" ms");
                candidates.clear();
                deviceResult = index_STS3(queryMapSignature, topK);
                break;
            case "josie":
                long time11 = System.currentTimeMillis();
                postingInvertedIndex = Josie.createJosiePostinglists(datasetIdMapSignature);
                long totalIndexByte2 = 0;
                totalIndexByte2 += 8*postingInvertedIndex.size();
                for (long i: postingInvertedIndex.keySet()){
                    TreeMap<String, int[]> tm = postingInvertedIndex.get(i);
                    totalIndexByte2 += 4 * tm.size();
                    for (String i2: tm.keySet()){
                        int[] INT = tm.get(i2);
                        totalIndexByte2 += 4 * INT.length;
                    }
                }
                System.out.println("totalIndexByte of Josie = "+ totalIndexByte2);
                long time22 = System.currentTimeMillis();
                System.out.println("silo:"+siloName+", josie posting list created time = "+(time22-time11)+" ms");
                candidates.clear();
                deviceResult = Josie.josie(postingInvertedIndex, datasetIdMapSignature,  queryMapSignature, topK );
                break;
            case "quadTree":
                long timee1 = System.currentTimeMillis();
                QuadTree<String> qt = quad_tree.buildTree(datasetIdMapSignature);
                long timee2 = System.currentTimeMillis();
                System.out.println("silo:"+siloName+", quadTree created time = "+(timee2-timee1)+" ms");
                IntersectNodeList.clear();
                deviceResult = quadTreeSearch(topK);
                break;
            case "RTree":
                long timee11 = System.currentTimeMillis();
                RTree tree = buildRTree(datasetIdMapSignature);
                long timee22 = System.currentTimeMillis();
                System.out.println("silo:"+siloName+", RTree created time = "+(timee22-timee11)+" ms");
                candidates.clear();
                RTDirNode root = (RTDirNode) tree.root;
                RTreeTraverse(root);
                System.out.println("Rtree candidates.size() = "+candidates.size());
                deviceResult = RTreeSearch(topK);

                break;
            case "sliceExactSearch" :
                candidates.clear();
                Set<String> CandidateSet = datasetIdMapSignature.keySet();
                for (String s: CandidateSet){
                    candidates.add(s);
                }
                deviceResult = slicePruneExactSearch(topK, candidates);
                break;
            case "exactSearch" :
                candidates.clear();
                Set<String> CandidateSet1 = datasetIdMapSignature.keySet();
                for (String s: CandidateSet1){
                    candidates.add(s);
                }
                deviceResult = exactDeviceSearch(topK, candidates);
                break;
            case "MBRBoundSearch":
                candidates.clear();
                BranchAndBound(dataLakeRoot, queryNode);
                deviceResult = MBRdeviceSearch(topK, candidates);
                break;
            case "IBtree":
                candidates.clear();
                deviceResult = IBtreeSearch(topK, dataLakeRoot, queryNode);
                break;
            case "sliceSearch":
                candidates.clear();
                BranchAndBound(dataLakeRoot, queryNode);
//                System.out.println("siloName: "+this.siloName+", "+"candidateset.size() = "+ candidates.size());
                deviceResult = sliceDeviceSearch(topK, candidates);
                break;
            default :
                System.out.println("unknown algorithm name");
        }

        return deviceResult;
    }

    public RTree buildRTree(HashMap<String, ArrayList<Long>> datasetIdMapSignature){
        RTree tree = new RTree(capacity, 0.5f, Constants.RTREE_QUADRATIC, dimension); //
        for (String s : datasetIdMapSignature.keySet()) {
            double minX, maxX, minY, maxY;
            minX = Double.MAX_VALUE;
            minY = Double.MAX_VALUE;
            maxX = -1000000000;
            maxY = -1000000000;
            ArrayList<Long> zcodeHm = datasetIdMapSignature.get(s);
            for (int i = 0; i<zcodeHm.size(); i++) {
                long l = zcodeHm.get(i);
                double[] d = Silo.resolve(l, Silo.resolution);
                if (minX > d[0]) {
                    minX = d[0];
                }
                if (minY > d[1]) {
                    minY = d[1];
                }
                if (maxX < d[0]) {
                    maxX = d[0];
                }
                if (maxY < d[1]) {
                    maxY = d[1];
                }
            }
//            System.out.print("s = "+s+";    "+ minX+", "+minY+"; "+maxX+", "+maxY+";    ");
            rtree.Point p1 = new rtree.Point(new double[] { minX, minY });
            rtree.Point p2 = new rtree.Point(new double[] { maxX, maxY });
            final Rectangle rectangle = new Rectangle(p1, p2, s);
            boolean b = tree.insert(rectangle);
//            System.out.println("b = "+b);
        }
        return tree;
    }

    public static boolean intersected(Rectangle rec, indexNode query, int dim) {
        double[] mbrmin = rec.getLow().data;
        double[] mbrmax = rec.getHigh().data;
        double[] querymin = query.getMBRmin();
        double[] querymax = query.getMBRmax();
        for(int i=0; i<dim; i++)
            if(mbrmin[i] > querymax[i] || mbrmax[i] < querymin[i]) {
                return false;
            }
        return true;
    }

    public void RTreeTraverse(RTDirNode root){
        List<RTNode> list = root.children;
        System.out.println("list.size() = "+list.size());
        for (RTNode r1: list){
            if (r1.isRoot() || r1.isIndex()){
                if (intersected(r1.getNodeRectangle(), queryNode, dimension)){
                    RTDirNode r2 = (RTDirNode) r1;
                    RTreeTraverse(r2);
                }

            }else if (r1.isLeaf()){
                Rectangle[] rects1 = r1.datas;
                for (Rectangle rects2: rects1){
                    if (rects2 != null) {
                        System.out.println(rects2.getId());
                        candidates.add(rects2.getId());
                    }
                }
            }
        }
    }


    public PriorityQueue<relaxIndexNode> RTreeSearch(int topK){
        PriorityQueue<relaxIndexNode> resultApprox = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
            @Override
            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                return (o2.getLb()-o1.getLb()>0)?1:-1;
            }
        }); //large - small
        PriorityQueue<relaxIndexNode> returnResult = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
            @Override
            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                return (o2.getLb()-o1.getLb()>0)?1:-1;
            }
        }); //large - small
        HashMap<String, Integer> tm = new HashMap<>();
        for (String candi: candidates){
            ArrayList<Long> arrayList = datasetIdMapSignature.get(candi);
            int count = 0;
            for (long l: arrayList){
                for (long q: queryMapSignature){
                    if (l == q){
                        count++;
                    }
                }
            }
            tm.put(candi, count);
        }

        for (String t: tm.keySet()){
            relaxIndexNode re = new relaxIndexNode(t, tm.get(t));
            resultApprox.add(re);
        }
        int num = Math.min(topK, resultApprox.size());
        for (int count2 = 0; count2<num; count2++){
            relaxIndexNode re = resultApprox.poll();
            returnResult.add(re);
        }
        return returnResult;
    }
    public PriorityQueue<relaxIndexNode> quadTreeSearch(int topK){
        PriorityQueue<relaxIndexNode> resultApprox = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
            @Override
            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                return (o2.getLb()-o1.getLb()>0)?1:-1;
            }
        }); //large - small
        PriorityQueue<relaxIndexNode> returnResult = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
            @Override
            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                return (o2.getLb()-o1.getLb()>0)?1:-1;
            }
        }); //large - small
        HashMap<String, Integer> tm = new HashMap<>();
        IntersectNodeList = quad_tree.getAllInterSectNode(queryNode);
        System.out.println("IntersectNodeList.size() = "+IntersectNodeList.size());
        for (int i=0; i<IntersectNodeList.size(); i++) {
            Node<String> node = IntersectNodeList.get(i);
            List<Point<String>> list= node.getPointList();
            for (Point<String> p: list){
                double[] d = {p.getX(), p.getY()};
                long l = combine(d, resolution);
                if (queryMapSignature.contains(l)){
                    int t = 0;
                    if (tm.containsKey(p.getValue())){
                        t = tm.get(p.getValue());
                    }
                    t++;
                    tm.put((String) p.getValue(), t);
                }
            }
        }

        for (String t: tm.keySet()){
            relaxIndexNode re = new relaxIndexNode(t, tm.get(t));
            resultApprox.add(re);
        }
        int num = Math.min(topK, resultApprox.size());
        for (int count2 = 0; count2<num; count2++){
            relaxIndexNode re = resultApprox.poll();
            returnResult.add(re);
        }
        return returnResult;

    }

    public void BranchAndBoundMCP(indexNode root, indexNode query){
        if (root.getroot() == -1 ) {// leaf node
            if (distance(root.getPivot(), query.getPivot()) - root.getRadius()
                    - query.getRadius() <= connectedThreshold){
                Set<String> S =  root.getNodeIDList();
                for (String id: S){
                    candidates.add(id);
                }
            }
        }else if(root.getroot() == -2){ //internal node
            Set<indexNode> listnode = root.getNodelist();
            for (indexNode aListNode: listnode){
                if (distance(aListNode.getPivot(), query.getPivot())
                        - aListNode.getRadius() - query.getRadius() <= connectedThreshold){
                    BranchAndBoundMCP(aListNode,query);
                }
            }
        }
//        System.out.println("candidateset.size() = "+candidateset.size());

    }

    public void BranchAndBound(indexNode root, indexNode query){
        if (root.getroot() == -1 ) {// leaf node
            if (indexDSS.intersected(root, query, dimension)){
                Set<String> S =  root.getNodeIDList();
                for (String id: S){
                    candidates.add(id);
                }
            }
        }else if(root.getroot() == -2){ //internal node
            Set<indexNode> listnode = root.getNodelist();
            for (indexNode aListNode: listnode){
                if (!indexDSS.intersected(aListNode, query, dimension)){
//                    System.out.println("prune");
                }
                else{
                    BranchAndBound(aListNode,query);
                }
            }
        }
//        System.out.println("candidateset.size() = "+candidateset.size());

    }


    public HashSet<indexNode> candidatesLeafnode = new HashSet<>();
    public PriorityQueue<relaxIndexNode> IBtreeSearch(int topk, indexNode dataLakeRoot, indexNode queryNode){
        IBBranchAndBound(dataLakeRoot, queryNode);
        PriorityQueue<relaxIndexNode> resultApprox = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
            @Override
            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                return (o1.getLb()-o2.getLb()>0)?1:-1;
            }
        }); //large - small
        double threshold = -1000000000;
        int prune = 0;
//        System.out.println("candidatesLeafnode.size() = "+candidatesLeafnode.size());
        for (indexNode in: candidatesLeafnode) {
            TreeMap<Long, HashSet<String>> pl = in.getPostinglist();
            HashMap<String, Integer> tempResult = new HashMap();
            for (long i : queryMapSignature) {
                if (pl.containsKey(i)) {
                    HashSet<String> posting = pl.get(i);
                    for (String p : posting) {
                        Integer frequence = tempResult.getOrDefault(p, new Integer(0));
                        frequence += 1;
                        tempResult.put(p, frequence);
                    }
                }
            }

            for (String i: tempResult.keySet()){
//                System.out.println(i+", "+ tempResult.get(i));
//                System.out.println("resultApprox.size() = "+resultApprox.size());
//                System.out.println("tempResult.size() = "+tempResult.size());
//                System.out.println("topk = "+ topK );
                if (resultApprox.size()>= topk){
                    threshold = resultApprox.peek().getLb();
                    int section = tempResult.get(i);
                    if (section <= threshold) {
                        prune++;
                    } else {
                        resultApprox.poll();
                        relaxIndexNode ri = new relaxIndexNode(i, section);
                        resultApprox.add(ri);
                    }
                }else {
                    int section = tempResult.get(i);
                    relaxIndexNode ri = new relaxIndexNode(i, section);
                    resultApprox.add(ri);
                }
            }
        }
        System.out.println("resultApprox.size() = "+resultApprox.size());
        return resultApprox;

    }

    public void IBBranchAndBoundMCP(indexNode root, indexNode query){
        if (root.getroot() == -1 ) {// leaf node
            if (distance(root.getPivot(), query.getPivot()) - root.getRadius()
                    - query.getRadius() <= connectedThreshold){
                candidatesLeafnode.add(root);
            }
        }else if(root.getroot() == -2){ //internal node
            Set<indexNode> listnode = root.getNodelist();
            for (indexNode aListNode: listnode){
                if(distance(aListNode.getPivot(), query.getPivot())
                        - aListNode.getRadius() - query.getRadius() <= connectedThreshold){
                    IBBranchAndBoundMCP(aListNode,query);
                }
            }
        }

    }


    public void IBBranchAndBound(indexNode root, indexNode query){
        if (root.getroot() == -1 ) {// leaf node
            if (indexDSS.intersected(root, query, dimension)){
                //1,计算相交MBR
                //直接计算与S的posting list并得到结果
                //1，遍历query,看token是否包含这个，遍历posting list就可以找到结果
                double[][] commonMBR = new double[2][2];
                double interNum = intersectedAreaMBR(root, query, commonMBR, dimension);
                if (interNum>0){
                    candidatesLeafnode.add(root);
                }
            }
        }else if(root.getroot() == -2){ //internal node
            Set<indexNode> listnode = root.getNodelist();
            for (indexNode aListNode: listnode){
                if (!indexDSS.intersected(aListNode, query, dimension)){

                }
                else{
                    IBBranchAndBound(aListNode,query);
                }
            }
        }

    }

    public String queryId;

    public void getQueryFromResponse(String response){
        //清空
        queryMapSignature.clear();
        queryHistogramSignature.clear();
        queryNode = new indexNode(dimension);
        querySlice = new HashMap<>();
        queryHistogramSlice = new HashMap<>();
//        System.out.println("response = "+response);

        String[] vals = response.split(":");

        String queryMBR = vals[0];
        String[] MBR = queryMBR.split(",");
        queryId = MBR[0];
        System.out.println("queryId = "+queryId);
        double[] queryMBRMax={Double.parseDouble(MBR[1]), Double.parseDouble(MBR[2])};
        double[] queryMBRMin={Double.parseDouble(MBR[3]), Double.parseDouble(MBR[4])};
        queryNode.setNodeid(queryId);
        queryNode.setMBRmin(queryMBRMin);
        queryNode.setMBRmax(queryMBRMax);
        double[] pivot = {(queryMBRMax[0]+queryMBRMin[0])/2, (queryMBRMax[1]+queryMBRMin[1])/2};
        queryNode.setPivot(pivot);
        queryNode.setRadius(distance(queryMBRMax, pivot));

        String queryString = vals[1];
        String queryHistogramString= vals[2];
        String[] queryID = queryString.split(",");
        String[] queryDensity = queryHistogramString.split(",");

        System.out.println("query.size = "+ queryID.length+", "+queryDensity.length);

        int n = queryID.length;
        for (int i = 0; i < n ; i++){
            Long gridID = Long.parseLong(queryID[i]);
            int density = Integer.parseInt(queryDensity[i]);
            queryMapSignature.add(gridID);
            queryHistogramSignature.put(gridID, density);
            double[] d = resolve(gridID, resolution);
            HashSet<Integer> hs = querySlice.getOrDefault((int)d[0], new HashSet<>());
            HashMap<Integer, Integer> hm = queryHistogramSlice.getOrDefault((int)d[0], new HashMap<>());
            hs.add((int)d[1]);
            hm.put((int)d[1], density);
            querySlice.put((int) d[0], hs);
            queryHistogramSlice.put((int) d[0], hm);
        }

    }

    public static int intersectedAreaMBR(indexNode root, indexNode query, double[][] interMBR, int dim) {
        double[] mbrmin = root.getMBRmin();
        double[] mbrmax = root.getMBRmax();
        double[] querymin = query.getMBRmin();
        double[] querymax = query.getMBRmax();
        if (!indexAlgorithm.intersected(root, query, dim)) { //如果相交则计算相交面积
            return 0;
        } else {
            if (querymin[0]<= mbrmin[0]){
                interMBR[1][0] = mbrmin[0];
                interMBR[1][1] = Math.max(querymin[1], mbrmin[1]);
                interMBR[0][0] = Math.min(querymax[0], mbrmax[0]);
                interMBR[0][1] = Math.min(querymax[1], mbrmax[1]);
            }else{
                interMBR[1][0] = querymin[0];
                interMBR[1][1] = Math.max(querymin[1], mbrmin[1]);
                interMBR[0][0] = Math.min(querymax[0], mbrmax[0]);
                interMBR[0][1] = Math.min(querymax[1], mbrmax[1]);
            }
            int area = (int) ((interMBR[0][0] - interMBR[1][0] + 1)* (interMBR[0][1] - interMBR[1][1] + 1));
            return area;
        }
    }



    public  int sliceUpperBound(double threshold, HashMap<Integer, HashSet<Integer>> querySlice, HashMap<Integer, HashSet<Integer>> dataSlice,
                                String candidateID, HashMap<Integer, HashSet<Integer>> newDataset, HashMap<Integer, HashSet<Integer>> newQuery){
        //传输的时候把queryNode的相关信息也传输过来，直接new一个queryNode,
        int upperbound = 0;
        //1,计算相交面积
        double[][] commonMBR = new double[2][2];
        double interNum = intersectedAreaMBR(datalakeRootNodes.get(candidateID), queryNode, commonMBR, dimension);
        if (interNum < threshold){
            upperbound = 0;
        }else{
            //2,找到落在相交面积里的点和新的MBR
            int dataNumFallMBR = 0;
            int queryNumFallMBR = 0;
            int minX = (int) commonMBR[1][0];
            int minY = (int) commonMBR[1][1];
            int maxX = (int) commonMBR[0][0];
            int maxY = (int) commonMBR[0][1];
            for (int i = minX; i<=maxX; i++){
                if (querySlice.containsKey(i) && dataSlice.containsKey(i)){
                    HashSet<Integer> hs_query = querySlice.get(i);
                    HashSet<Integer> new_hs_query = new HashSet<>();
                    for (int hs_id: hs_query){
                        if (hs_id >= minY && hs_id <= maxY){
                            queryNumFallMBR++;
                            new_hs_query.add(hs_id);
                        }
                    }

                    HashSet<Integer> hs_data = dataSlice.get(i);
                    HashSet<Integer> new_hs_data = new HashSet<>();
                    for (int hs_id: hs_data){
                        if (hs_id >= minY && hs_id <= maxY){
                            dataNumFallMBR++;
                            new_hs_data.add(hs_id);
                        }
                    }
                    newDataset.put(i, new_hs_data);
                    newQuery.put(i, new_hs_query);

                }
            }
            upperbound = Math.min(dataNumFallMBR, queryNumFallMBR);
        }
        return upperbound;
    }

    public  int sliceUpperBound(double threshold, HashMap<Integer, HashSet<Integer>> querySlice, HashMap<Integer, HashSet<Integer>> dataSlice,
                                String candidateID, int[][] MBR, HashMap<Integer, HashSet<Integer>> newDataset, HashMap<Integer, HashSet<Integer>> newQuery){
        //传输的时候把queryNode的相关信息也传输过来，直接new一个queryNode,
        int upperbound = 0;
        //1,计算相交面积
        double[][] commonMBR = new double[2][2];
        double interNum = intersectedAreaMBR(datalakeRootNodes.get(candidateID), queryNode, commonMBR, dimension);
        if (interNum < threshold){
            upperbound = 0;
        }else{
            //2,找到落在相交面积里的点和新的MBR
            int dataNumFallMBR = 0;
            int queryNumFallMBR = 0;
            int minX = (int) commonMBR[1][0];
            int minY = (int) commonMBR[1][1];
            int maxX = (int) commonMBR[0][0];
            int maxY = (int) commonMBR[0][1];
            MBR[1][0] = minX;  MBR[1][1] = minY;  MBR[0][0] = maxX; MBR[0][1] = maxY;
            for (int i = minX; i<=maxX; i++){
                if (querySlice.containsKey(i) && dataSlice.containsKey(i)){
                    HashSet<Integer> hs_query = querySlice.get(i);
                    HashSet<Integer> new_hs_query = new HashSet<>();
                    for (int hs_id: hs_query){
                        if (hs_id >= minY && hs_id <= maxY){
                            queryNumFallMBR++;
                            new_hs_query.add(hs_id);
                        }
                    }

                    HashSet<Integer> hs_data = dataSlice.get(i);
                    HashSet<Integer> new_hs_data = new HashSet<>();
                    for (int hs_id: hs_data){
                        if (hs_id >= minY && hs_id <= maxY){
                            dataNumFallMBR++;
                            new_hs_data.add(hs_id);
                        }
                    }
                    newDataset.put(i, new_hs_data);
                    newQuery.put(i, new_hs_query);

                }
            }
            upperbound = Math.min(dataNumFallMBR, queryNumFallMBR);
        }
        return upperbound;
    }


    public  int upperBoundCompute(double threshold, ArrayList<Long> queryMapSignature, ArrayList<Long> dataset, String candidateID, ArrayList<Long> newQuery, ArrayList<Long> newData){
        //传输的时候把queryNode的相关信息也传输过来，直接new一个queryNode,
        int upperbound = 0;
        //1,计算相交面积
        double[][] commonMBR = new double[2][2];
        double interNum = intersectedAreaMBR(datalakeRootNodes.get(candidateID), queryNode, commonMBR, dimension);
        if (interNum < threshold){
            upperbound = 0;
        }else{
            //2,找到落在相交面积里的点和新的MBR
            int dataNumFallMBR = 0;
            int queryNumFallMBR = 0;
//        double[][] dataNewMBR = new double[2][2];
//        double[][] queryNewMBR = new double[2][2];
            for (int i = 0; i<queryMapSignature.size(); i++){
                double[] coord = resolve(queryMapSignature.get(i), resolution);
                if (coord[0]>= commonMBR[1][0] && coord[0]<= commonMBR[0][0] && coord[1]>= commonMBR[1][1] && coord[1]<= commonMBR[0][1]){
                    dataNumFallMBR++;
                    newQuery.add(queryMapSignature.get(i));
                }
            }
            for (int i = 0; i<dataset.size(); i++){
                double[] coord = resolve(dataset.get(i), resolution);
                if (coord[0]>= commonMBR[1][0] && coord[0]<= commonMBR[0][0] && coord[1]>= commonMBR[1][1] && coord[1]<= commonMBR[0][1]){
                    queryNumFallMBR++;
                    newData.add(dataset.get(i));
                }
            }
            //3, 如果新的MBR不相交，则返回0，否则等于Math.min(a，b)

            upperbound = Math.min(dataNumFallMBR, queryNumFallMBR);
        }
        return upperbound;
    }

    public PriorityQueue<relaxIndexNode> sliceDeviceSearch(int topk, HashSet<String> candidateSet) throws CloneNotSupportedException{
        PriorityQueue<relaxIndexNode> resultApprox = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
            @Override
            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                return (o1.getLb()-o2.getLb()>0)?1:-1;
            }
        }); //large - small
        double threshold = -1000000000;
        int prune = 0;
        for (String candidateID: candidateSet){
//            System.out.println( "candidateID = "+candidateID);
            ArrayList<Long> dataset = datasetIdMapSignature.get(candidateID);
            HashMap<Integer, HashSet<Integer>> dataSlice = xSliceInformation.get(candidateID);
            HashMap<Integer, HashSet<Integer>> newDataset = new HashMap<>();
            HashMap<Integer, HashSet<Integer>> newQuery = new HashMap<>();

            if (resultApprox.size()>=topk){
                threshold = resultApprox.peek().getLb();
                int upperbound = sliceUpperBound(threshold, querySlice, dataSlice, candidateID, newDataset, newQuery);
                if (upperbound <= threshold){
                    prune++;
                }else {
                    //（1）继续细化 还是（2）直接准确计算？
                    int newSetIntersection = 0;
                    for (int i: newQuery.keySet()){
                        HashSet<Integer> s_data = newDataset.get(i);
                        HashSet<Integer> s_query = newQuery.get(i);
                        if (s_data.size()>0 && s_query.size()>0) {
                            for (int j : s_query) {
                                for (int z : s_data) {
                                    if (j == z) {
                                        newSetIntersection++;
                                    }
                                }
                            }
                        }
                    }
                    if (newSetIntersection > threshold){
                        resultApprox.poll();
                        relaxIndexNode in = new relaxIndexNode(candidateID, newSetIntersection);
                        resultApprox.add(in);
                    }
                }

            }else {
                int setIntersection = exactCompute(queryMapSignature,dataset);
                relaxIndexNode in = new relaxIndexNode(candidateID, setIntersection);
                resultApprox.add(in);
                relaxIndexNode re = resultApprox.peek();
            }

        }
//        System.out.println("prune = "+prune);
        return resultApprox;

    }

    public PriorityQueue<relaxIndexNode> slicePruneExactSearch(int topk, HashSet<String> candidateSet) throws CloneNotSupportedException{
        PriorityQueue<relaxIndexNode> resultApprox = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
            @Override
            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                return (o1.getLb()-o2.getLb()>0)?1:-1;
            }
        }); //small - large
        double threshold = -1000000000;
        int prune = 0;
        for (String candidateID: candidateSet){
//            System.out.println( "candidateID = "+candidateID);
            ArrayList<Long> dataset = datasetIdMapSignature.get(candidateID);
            HashMap<Integer, HashSet<Integer>> dataSlice = xSliceInformation.get(candidateID);
            HashMap<Integer, HashSet<Integer>> newDataset = new HashMap<>();
            HashMap<Integer, HashSet<Integer>> newQuery = new HashMap<>();

            if (resultApprox.size()>=topk){
                threshold = resultApprox.peek().getLb();
                int upperbound = sliceUpperBound(threshold, querySlice, dataSlice, candidateID, newDataset, newQuery);
                if (upperbound <= threshold){
                    prune++;
                }else {
                    //（1）继续细化 还是（2）直接准确计算？
                    int newSetIntersection = 0;
                    for (int i: newQuery.keySet()){
                        TreeSet<Integer> s_data = new TreeSet<>(newDataset.get(i)); //small -> large
                        TreeSet<Integer> s_query = new TreeSet<>(newQuery.get(i));  //small -> large
                        if (s_data.size()>0 && s_query.size()>0) {
                            if (s_query.first() >= s_data.first()){
                                if (s_query.last() >= s_data.first()){
                                    Iterator iterator = s_data.iterator();
                                    while (iterator.hasNext()){
                                        int j = (int)iterator.next();
                                        if (j <= s_query.last() ){
                                            if (s_query.contains(j))
                                                newSetIntersection++;
                                        }
                                        else break;
                                    }
                                }
                            }else {
                                if (s_query.first() >= s_data.first()){
                                    if (s_query.first() >= s_data.last()){
                                        Iterator iterator = s_query.iterator();
                                        while (iterator.hasNext()){
                                            int j = (int)iterator.next();
                                            if (j <= s_data.last()){
                                                if (s_data.contains(j))
                                                    newSetIntersection++;
                                            }else break;
                                        }
                                    }
                                }

                            }
                        }
                    }
                    if (newSetIntersection > threshold){
                        resultApprox.poll();
                        relaxIndexNode in = new relaxIndexNode(candidateID, newSetIntersection);
                        resultApprox.add(in);
                    }
                }

            }else {
                int setIntersection = exactCompute(queryMapSignature,dataset);
                relaxIndexNode in = new relaxIndexNode(candidateID, setIntersection);
                resultApprox.add(in);
                relaxIndexNode re = resultApprox.peek();
            }

        }
//        System.out.println("prune = "+prune);
        return resultApprox;

    }


    public PriorityQueue<relaxIndexNode> MBRdeviceSearch(int topk, HashSet<String> candidateSet) throws CloneNotSupportedException{
        // 利用过滤的办法
        /*
           1, 对于前五个数据集 直接存入priority queue
           2, 后续第一步是计算相交面积，如果相交数量小于阈值，直接过滤；
           3，如果相交面积数量大于阈值:
               (1) 计算落在相交面积内的点并找到新的MBR，计算 set intersection = min(a, b), 判断是否过滤;
               (2)
        */
//        System.out.println("candidateSet = "+candidateSet);
        PriorityQueue<relaxIndexNode> resultApprox = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
            @Override
            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                return (o1.getLb()-o2.getLb()>0)?1:-1;
            }
        }); //small - large
        double threshold = -1000000000;
        int prune = 0;
        for (String candidateID: candidateSet){
//            System.out.println( "candidateID = "+candidateID);
            ArrayList<Long> dataset = datasetIdMapSignature.get(candidateID);
            ArrayList<Long> newDataset = new ArrayList<>();
            ArrayList<Long> newQuery = new ArrayList<>();
            if (resultApprox.size()>=topk){
                threshold = resultApprox.peek().getLb();
                int upperbound = upperBoundCompute(threshold,queryMapSignature, dataset, candidateID,newQuery,newDataset);
//                System.out.println("threshold = "+threshold+", upperbound = "+upperbound);
                if (upperbound <= threshold){
                    prune++;
                }else {
                    //（1）继续细化 还是（2）直接准确计算？
                    int newSetIntersection = exactCompute(newQuery,newDataset);
//                    System.out.println("candidateID = "+candidateID+", newSetIntersection = "+newSetIntersection);
                    if (newSetIntersection > threshold){
                        resultApprox.poll();
                        relaxIndexNode in = new relaxIndexNode(candidateID, newSetIntersection);
                        resultApprox.add(in);
                    }
                }

            }else {
                int setIntersection = exactCompute(queryMapSignature,dataset);
//                System.out.println("<topk = "+resultApprox.size() +", candidate = "+candidateID+", setIntersection = "+setIntersection);
                relaxIndexNode in = new relaxIndexNode(candidateID, setIntersection);
                resultApprox.add(in);
//                System.out.println("in.getID = "+in.getResultId()+", in.getLB = "+in.getLb());
//                System.out.println(resultApprox.size());
                relaxIndexNode re = resultApprox.peek();
//                System.out.println("resultApprox.peek = "+re.getLb());
            }

        }
//        System.out.println("prune = "+prune);
        return resultApprox;

    }

    public PriorityQueue<relaxIndexNode> exactDeviceSearch(int topk, HashSet<String> candidateSet) throws CloneNotSupportedException{
        PriorityQueue<relaxIndexNode> resultApprox = new PriorityQueue<>(new ComparatorByRelaxIndexNodeReverse()); // small- large
        double threshold;
        for (String candidateID: candidateSet){
            ArrayList<Long> dataset = datasetIdMapSignature.get(candidateID);
            int setIntersection = sortedExactCompute(queryMapSignature,dataset);
            if (setIntersection>0){
//                System.out.println("candidateID = "+candidateID+": intersection = "+setIntersection+"; ");
                if (resultApprox.size()>=topk){
                    threshold = resultApprox.peek().getLb();
                    if (threshold < setIntersection){
                        threshold = setIntersection;
                        resultApprox.poll();
                        relaxIndexNode in = new relaxIndexNode(candidateID, setIntersection);
                        resultApprox.add(in);
                    }
                }else {
                    relaxIndexNode in = new relaxIndexNode(candidateID, setIntersection);
                    resultApprox.add(in);
                }
            }else {
                relaxIndexNode in = new relaxIndexNode(candidateID, setIntersection);
                resultApprox.add(in);
            }
        }
        return resultApprox;
    }

    public static int intersectedArea(indexNode root, indexNode query, int dim) {
        double[] mbrmin = root.getMBRmin();
        double[] mbrmax = root.getMBRmax();
        double[] querymin = query.getMBRmin();
        double[] querymax = query.getMBRmax();
        if (!indexAlgorithm.intersected(root, query, dim)) { //如果相交则计算相交面积
            return 0;
        } else {
            int area = 1;
            for (int i = 0; i < dim; i++) {
                if (querymin[i]<= mbrmin[i]){
                    area *=  Math.min(querymax[i], mbrmax[i]) - mbrmin[i] + 1;
                }else{
                    area *= Math.min(querymax[i], mbrmax[i])- querymin[i] + 1;
                }
            }
            return Math.abs(area);
        }
    }

    public static int exactCompute(ArrayList<Long> query, ArrayList<Long> dataset){
        if (query.isEmpty() || dataset.isEmpty()){
            return 0;
        }else {
            int setIntersection = 0;
            for (long l_query : query) {
                for (long l_dataset : dataset) {
                    if (l_query == l_dataset) {
                        setIntersection++;
                    }
                }
            }
            return setIntersection;
        }
    }

    public static int sortedExactCompute(ArrayList<Long> query, ArrayList<Long> dataset){
        if (query.isEmpty() || dataset.isEmpty()){
            return 0;
        }else {
            //1, 排序
            Long[] querySort = new Long[query.size()];
            for (int i = 0; i< query.size(); i++){
                querySort[i] = query.get(i);
            }
            Long[] datasetSort = new Long[dataset.size()];
            for (int i = 0; i< dataset.size(); i++){
                datasetSort[i] = dataset.get(i);
            }
            Arrays.sort(querySort);
            Arrays.sort(datasetSort);

            int setIntersection = 0;
            for (int i = 0; i<querySort.length; i++){
                long key = querySort[i];
                int low = 0;
                int high = datasetSort.length-1;
                if(key < datasetSort[low] || key > datasetSort[high]){

                }else {
                    while ( low <= high){
                        int middle = (low+high)/2;
                        if (datasetSort[middle] > key){
                            high = middle - 1;
                        }else if (datasetSort[middle] < key){
                            low = middle + 1;
                        }else if (datasetSort[middle] == key){
                            setIntersection++;
                        }
                    }
                }
            }

//            for (long l_query : query) {
//                for (long l_dataset : dataset) {
//                    if (l_query == l_dataset) {
//                        setIntersection++;
//                    }
//                }
//            }
            return setIntersection;
        }
    }

    public HashMap<String, HashMap<Long,Double>> generateDatasetIDList(
            HashMap<String, ArrayList<Long>> datasetIdMapSignature, HashMap<String, indexNode> datalakeRootNodes,
            HashMap<String, indexNode> indexNodesHM, ArrayList<indexNode> indexNodes,HashMap<String, double[][]> zcodeMBR,
            int dimension, String siloID, String siloName,String Path,int filecount,
            double minX, double minY, double rangeX, double rangeY, int resolution,
            HashMap<String, HashMap<Integer, HashSet<Integer>>> xSliceInformation) throws IOException, CloneNotSupportedException {

        String signaturePath = preprocessFilePath+ "silo-" + siloName +"/";
        File f =new File(signaturePath);
//        System.out.println("file exist ? "+ f .exists());
        if  (!f .exists()  && !f.isDirectory()) {
            f.mkdir();
        }
        HashMap<String, HashMap<Long, Double>> zcodeMap = new HashMap<String, HashMap<Long, Double>>();
        String filePath = signaturePath  + "file-" + filecount+"-resolution-"+resolution + ".txt";
//        System.out.println("the address of spatial sets = "+filePath );
        try {
            File file = new File(filePath);
            if (file.exists()) {
                //直接读文件
//                System.out.println("file = "+filePath+"is exist!!!");
                try(BufferedReader br = new BufferedReader(new FileReader(filePath))){
                    String strLine;
                    while ((strLine = br.readLine()) != null) {
                        HashMap<Long, Double> zcodeHashMap = new HashMap<>();
                        ArrayList<Long> arrayList = new ArrayList<>();
                        String[] splitString = strLine.split(";");
//                        int id = Integer.parseInt(splitString[0]);
                        String[] idAndMbr = splitString[0].split(",");
                        String id = idAndMbr[0];
                        double[][] MBRs = new double[2][2];
                        MBRs[0][0] = Double.parseDouble(idAndMbr[1]);
                        MBRs[0][1] = Double.parseDouble(idAndMbr[2]);
                        MBRs[1][0] = Double.parseDouble(idAndMbr[3]);
                        MBRs[1][1] = Double.parseDouble(idAndMbr[4]);
//                        System.out.println("MBRs = "+MBRs[0][0]+", "+MBRs[0][1]+", "+MBRs[1][0]+", "+MBRs[1][1]+"; ");
                        zcodeMBR.put(id, MBRs);
                        double max[], min[];
                        max = MBRs[0];
                        min = MBRs[1];
                        double pivot[] = new double[dimension];
                        double radius=0;

                        HashMap<Integer, HashSet<Integer>> sliceInformation = new HashMap<Integer, HashSet<Integer>>();
                        for (int i = 1; i<splitString.length; i++){
                            String[] hm = splitString[i].split(",");
                            zcodeHashMap.put(Long.parseLong(hm[0]), Double.parseDouble(hm[1]));
                            arrayList.add(Long.parseLong(hm[0]));
                            double[] datapoint = resolve(Long.parseLong(hm[0]), resolution);
                            HashSet sliceHS = sliceInformation.getOrDefault((int)datapoint[0], new HashSet<>());
                            sliceHS.add((int)datapoint[1]);
                            sliceInformation.put((int)datapoint[0], sliceHS);
//
//                            for(int j=0; j<dimension;j++) {
//                                pivot[j] += datapoint[j];
//                            }
                        }
                        xSliceInformation.put(id, sliceInformation);

                        for(int i=0; i<dimension;i++) {
                            radius += Math.pow((max[i] - min[i])/2, 2);
                        }

                        for (int i =0; i<dimension; i++){
                            pivot[i] = min[i]+max[i];
                        }
                        for (int i=0; i<dimension; i++){
                            pivot[i] = pivot[i]/2;
                        }

                        radius = Math.sqrt(radius);
                        indexNode rootNode = new indexNode(dimension);
                        rootNode.setMBRmax(max);
                        rootNode.setMBRmin(min);
                        rootNode.setRadius(radius);
                        rootNode.setPivot(pivot);
                        rootNode.setNodeid(id);
                        rootNode.setDeviceID(siloID);
                        zcodeMap.put(id, zcodeHashMap);
                        datasetIdMapSignature.put(id,arrayList);
                        datalakeRootNodes.put(id, rootNode);
                        indexNodes.add(rootNode);
                        indexNodesHM.put(id, rootNode);
                        //有dataPoint的坐标，有MBR可以构建quadtree了吧


                    }
                }catch (IOException e) {
                    e.printStackTrace();
                }
            }else {
                File filee = new File(Path);
                File[] files = filee.listFiles();
                for (File ff: files) {
                    String datasetid = ff.getName();
                    String dataPath = Path + datasetid;
                    double[][] MBR = new double[2][2];
                    HashSet<Long> hs = new HashSet<>();
                    HashMap<Long, Double> zcodeHashMap = generateSignature(hs, MBR, minX, minY, rangeX, rangeY, resolution, dataPath);
                    HashMap<Integer, HashSet<Integer>> sliceInformation = new HashMap<>();
                    if (zcodeHashMap.size() > 0) {
                        ArrayList<Long> arrayList = new ArrayList<>(hs);
                        double max[], min[], pivot[], radius = 0;
                        max = MBR[0];
                        min = MBR[1];
                        pivot = new double[dimension];
                        for (int i = 0; i < arrayList.size(); i++) {
                            double[] datapoint = resolve(arrayList.get(i), resolution);
                            HashSet sliceHS = sliceInformation.getOrDefault((int)datapoint[0], new HashSet<>());
                            sliceHS.add((int)datapoint[1]);
                            sliceInformation.put((int)datapoint[0], sliceHS);
//                            for (int j = 0; j < dimension; j++) {
//                                pivot[j] += datapoint[j];
//                            }

                        }
                        xSliceInformation.put(datasetid,sliceInformation);

                        for(int i=0; i<dimension;i++) {
                            radius += Math.pow((max[i] - min[i])/2, 2);
                        }

                        for (int i =0; i<dimension; i++){
                            pivot[i] = min[i]+max[i];
                        }
                        for (int i=0; i<dimension; i++){
                            pivot[i] = pivot[i]/2;
                        }

                        radius = Math.sqrt(radius);
                        indexNode rootNode = new indexNode(dimension);
                        rootNode.setMBRmax(max);
                        rootNode.setMBRmin(min);
                        rootNode.setRadius(radius);
                        rootNode.setPivot(pivot);
                        rootNode.setNodeid(datasetid);
                        rootNode.setDeviceID(siloID);
                        zcodeMap.put(datasetid, zcodeHashMap);
                        zcodeMBR.put(datasetid, MBR);
                        datasetIdMapSignature.put(datasetid, arrayList);
                        datalakeRootNodes.put(datasetid, rootNode);
                        indexNodes.add(rootNode);
                    }
                }


                DataLoader.writeFileAndMBR(zcodeMBR, filePath, zcodeMap);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return zcodeMap;
    }

    public HashMap<String, HashMap<Long,Double>> generateSignatureFile(
            HashMap<String, ArrayList<Long>> datasetIdMapSignature, HashMap<String, indexNode> datalakeRootNodes,
            HashMap<String, indexNode> indexNodesHM, ArrayList<indexNode> indexNodes,HashMap<String, double[][]> zcodeMBR,
            int dimension, String siloID, String siloName,String Path,
            double minX, double minY, double rangeX, double rangeY, int resolution,
            HashMap<String, HashMap<Integer, HashSet<Integer>>> xSliceInformation) throws IOException, CloneNotSupportedException {

        String signaturePath = preprocessFilePath+ siloName +"/";
        File f =new File(signaturePath);
        if  (!f.exists()  && !f.isDirectory()) {
            f.mkdir();
            System.out.println("create success");
        }
        HashMap<String, HashMap<Long, Double>> zcodeMap = new HashMap<String, HashMap<Long, Double>>();
        String filePath = signaturePath  + "resolution-"+resolution + ".txt";

        try {
            File file = new File(filePath);
            long time1 = System.currentTimeMillis();
            if (file.exists()) {
                //直接读文件
                System.out.println("file = "+filePath+" is exist!!!");
                try(BufferedReader br = new BufferedReader(new FileReader(filePath))){
                    String strLine;
                    while ((strLine = br.readLine()) != null) {
                        HashMap<Long, Double> zcodeHashMap = new HashMap<>();
                        ArrayList<Long> arrayList = new ArrayList<>();
                        String[] splitString = strLine.split(";");
//                        int id = Integer.parseInt(splitString[0]);
                        String[] idAndMbr = splitString[0].split(",");
                        String id = idAndMbr[0];
                        double[][] MBRs = new double[2][2];
                        MBRs[0][0] = Double.parseDouble(idAndMbr[1]);
                        MBRs[0][1] = Double.parseDouble(idAndMbr[2]);
                        MBRs[1][0] = Double.parseDouble(idAndMbr[3]);
                        MBRs[1][1] = Double.parseDouble(idAndMbr[4]);

                        zcodeMBR.put(id, MBRs);
                        double max[], min[];
                        max = MBRs[0];
                        min = MBRs[1];
                        double pivot[] = new double[dimension];
                        double radius=0;

                        HashMap<Integer, HashSet<Integer>> sliceInformation = new HashMap<Integer, HashSet<Integer>>();
                        for (int i = 1; i<splitString.length; i++){
                            String[] hm = splitString[i].split(",");
                            zcodeHashMap.put(Long.parseLong(hm[0]), Double.parseDouble(hm[1]));
                            arrayList.add(Long.parseLong(hm[0]));
                            double[] datapoint = resolve(Long.parseLong(hm[0]), resolution);
                            HashSet sliceHS = sliceInformation.getOrDefault((int)datapoint[0], new HashSet<>());
                            sliceHS.add((int)datapoint[1]);
                            sliceInformation.put((int)datapoint[0], sliceHS);
//                            for(int j=0; j<dimension;j++) {
//                                pivot[j] += datapoint[j];
//                            }
                        }
                        xSliceInformation.put(id, sliceInformation);

                        for(int i=0; i<dimension;i++) {
                            radius += Math.pow((max[i] - min[i])/2, 2);
                        }

                        for (int i =0; i<dimension; i++){
                            pivot[i] = min[i]+max[i];
                        }
                        for (int i=0; i<dimension; i++){
                            pivot[i] = pivot[i]/2;
                        }
                        radius = Math.sqrt(radius);
                        indexNode rootNode = new indexNode(dimension);
                        rootNode.setMBRmax(max);
                        rootNode.setMBRmin(min);
                        rootNode.setRadius(radius);
                        rootNode.setPivot(pivot);
                        rootNode.setNodeid(id);
                        rootNode.setDeviceID(siloID);
                        zcodeMap.put(id, zcodeHashMap);
                        datasetIdMapSignature.put(id, arrayList);
                        datalakeRootNodes.put(id,rootNode);
                        indexNodesHM.put(id, rootNode);
                        indexNodes.add(rootNode);


                    }
                }catch (IOException e) {
                    e.printStackTrace();
                }
            }else {
                File[] listFiles = new File(Path).listFiles();
                for (File filename: listFiles) {
                    String datasetid = filename.getName();
                    String dataPath = Path + datasetid;
                    double[][] MBR = new double[2][2];
                    HashSet<Long> hs = new HashSet<>();
                    HashMap<Long, Double> zcodeHashMap = generateSignature(hs, MBR, minX, minY, rangeX, rangeY, resolution, dataPath);
                    HashMap<Integer, HashSet<Integer>> sliceInformation = new HashMap<>();
                    if (zcodeHashMap.size() > 0) {
                        ArrayList<Long> arrayList = new ArrayList<>(hs);
                        double max[], min[], pivot[], radius = 0;
                        max = MBR[0];
                        min = MBR[1];
                        pivot = new double[dimension];
                        for (int i = 0; i < arrayList.size(); i++) {
                            double[] datapoint = resolve(arrayList.get(i), resolution);
                            HashSet sliceHS = sliceInformation.getOrDefault((int)datapoint[0], new HashSet<>());
                            sliceHS.add((int)datapoint[1]);
                            sliceInformation.put((int)datapoint[0], sliceHS);
//                            for (int j = 0; j < dimension; j++) {
//                                pivot[j] += datapoint[j];
//                            }
                        }
                        xSliceInformation.put(datasetid,sliceInformation);

                        for(int i=0; i<dimension;i++) {
                            radius += Math.pow((max[i] - min[i])/2, 2);
                        }

                        for (int i =0; i<dimension; i++){
                            pivot[i] = min[i]+max[i];
                        }
                        for (int i=0; i<dimension; i++){
                            pivot[i] = pivot[i]/2;
                        }

                        radius = Math.sqrt(radius);
                        indexNode rootNode = new indexNode(dimension);
                        rootNode.setMBRmax(max);
                        rootNode.setMBRmin(min);
                        rootNode.setRadius(radius);
                        rootNode.setPivot(pivot);
                        rootNode.setNodeid(datasetid);
                        rootNode.setDeviceID(siloID);
                        zcodeMap.put(datasetid, zcodeHashMap);
                        zcodeMBR.put(datasetid, MBR);
                        datasetIdMapSignature.put(datasetid, arrayList);
                        datalakeRootNodes.put(datasetid, rootNode);
                        indexNodes.add(rootNode);
                    }
                }
                DataLoader.writeFileAndMBR(zcodeMBR, filePath, zcodeMap);
            }
            long time2 = System.currentTimeMillis();
            System.out.println("read file time = "+(time2- time1));
        }catch(Exception e){
            e.printStackTrace();
        }
        return zcodeMap;
    }


    public static double[] resolve(Long key, int resolution){
        double[] d = new double[2];
        d[0] = (int)(key % Math.pow(2,resolution)); //x
        d[1] = (int)(key / Math.pow(2,resolution));  //y
        return d;
    }
    public static long combine(double[] coordi, int resolution){
        long d = 0;
        double t = Math.pow(2,resolution);
        d = (long)(coordi[1]*t+coordi[0]);
//        for (int i=0; i<coordi.length-1; i++){
//            d += (coordi[i]*t);
//        }
//        d+= coordi[coordi.length-1];
        return d;
    }


    public static void writeFile(String filePath, HashMap<Integer, HashMap<Long,Double>> dataSetMap) throws IOException {
        FileWriter fw = null;
        try {
            File file = new File(filePath);
            file.createNewFile();
            fw = new FileWriter(filePath);
            for (int id : dataSetMap.keySet()) {
                HashMap<Long,Double> map1 = dataSetMap.get(id);
                fw.write(String.valueOf(id));
                fw.write(";");
                for (long key: map1.keySet()){
                    fw.write(String.valueOf(key));
                    fw.write(",");
                    String aa = String.valueOf(map1.get(key));
                    BigDecimal db = new BigDecimal(aa);
                    String ii = db.toPlainString();
                    fw.write(ii+";");
                }
                fw.write("\n");
//                    System.out.println();
            }
            fw.close();

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public  HashMap<Long,Double> generateSignature(HashSet<Long> hs, double[][] MBR, double minX, double minY, double rangeX, double rangeY, int resolution, String Path) throws CloneNotSupportedException{
        double blockSizeX = rangeX/Math.pow(2,resolution);
        double blockSizeY = rangeY/Math.pow(2,resolution);
        HashMap<Long,Integer> ZorderHis = new HashMap<>();
        HashMap<Long,Double> ZorderDensityHist = new HashMap<>();
        long t = (long)Math.pow(2,resolution);
        if (siloName.startsWith("argoverse")){
            a = 6;
            b = 7;
            c = 2;
        }else if (siloName.equals("baidu")){
            a = 1; //lng
            b = 2; //lat
        }else{
            a = 0;
            b = 1;
        }
        double[] maxMBR = {-1000000000, -1000000000};
        double[] minMBR = {Double.MAX_VALUE, Double.MAX_VALUE};
        try {
            String record = "";
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(Path)));
            reader.readLine();
            while((record=reader.readLine())!=null) {
                String[] fields = record.split(",");
                double x_coordinate = Double.parseDouble(fields[a]);
                double y_coordinate = Double.parseDouble(fields[b]);
                if (fields[a].matches("-?\\d+(\\.\\d+)?") && x_coordinate>-180 && x_coordinate < 180 &&
                        y_coordinate> -90 && y_coordinate <90){
                    double x = x_coordinate - minX;
                    double y = y_coordinate - minY;
                    long X = (long) (x / blockSizeX);  //行row
                    long Y = (long) (y / blockSizeY);  //列 col
                    long id = Y * t + X;
                    hs.add(id);
                    Integer num = ZorderHis.getOrDefault(id, new Integer(0));
                    ZorderHis.put(id, num + 1);
                    ZorderDensityHist.put(id, (double) (num + 1));
                    if (maxMBR[0] < X)
                        maxMBR[0] = X;
                    if (maxMBR[1] < Y)
                        maxMBR[1] = Y;
                    if (minMBR[0] > X)
                        minMBR[0] = X;
                    if (minMBR[1] > Y)
                        minMBR[1] = Y;
                }
            }
            MBR[0] = maxMBR.clone();
            MBR[1] = minMBR.clone();
//            System.out.println(" MBR(gen) = "+maxMBR[0]+", "+maxMBR[1]+", "+minMBR[0]+", "+minMBR[1]);
            reader.close();
        }catch (IOException e) {
//            e.printStackTrace();
//            System.out.println("this dataset name isn't exist");
        }

        return ZorderDensityHist;
    }

    public PriorityQueue<relaxIndexNode> genePostingList(ArrayList<Long> queryMap){
        PriorityQueue<relaxIndexNode> allResults = new PriorityQueue<>(new ComparatorByRelaxIndexNode()); //large - small
        HashMap<String, Integer> resultHm = new HashMap<>();
        for (int i = 0; i< queryMap.size(); i++){
            long l = queryMap.get(i);
//            System.out.println("l = "+l+" : ");
            if (postinglists.containsKey(l)){
                HashSet<String> candiDatasetID = postinglists.get(l);
//                for (int j = 0; j < candiDatasetID.size(); j++){
//                    System.out.print(candiDatasetID.get(j)+", ");
//                }
                for (String j:candiDatasetID){
                    String datasetId = j;
                    Integer frequence = resultHm.getOrDefault(datasetId, new Integer(0));
                    frequence += 1;
                    resultHm.put(datasetId, frequence);
                }
            }

        }
        for (String i: resultHm.keySet()){
            relaxIndexNode re = new relaxIndexNode(i, resultHm.get(i));
            allResults.add(re);
        }

        PriorityQueue<relaxIndexNode> finalResultHeap = new PriorityQueue(new ComparatorByRelaxIndexNode()); // large-> small

        for (int count2 = 0; count2<topK; count2++){
            relaxIndexNode re = allResults.poll();
            finalResultHeap.add(re);
        }

        return finalResultHeap;

    }

    public static void SerializedZcurve(String file, HashMap<String, HashMap<String,Integer>> result) {
        try {
            FileOutputStream fos = new FileOutputStream(file);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(result);
            oos.close();
            fos.close();
            System.out.println("Serialized result HashMap data is saved in hashmap.ser");
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static HashMap<String, HashMap<String, Integer>> deSerializationZcurve(String file) {
        HashMap<String, HashMap<String, Integer>> result;
        try {
            FileInputStream fis = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fis);
            result = (HashMap) ois.readObject();
            ois.close();
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("Class not found");
            c.printStackTrace();
            return null;
        }
        return result;
    }



    public static List<indexNode> getAllIndexNode(File file) throws FileNotFoundException, IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(file);
        ObjectInputStream ois = new ObjectInputStream(fis);
        List<indexNode> objArr = new ArrayList<indexNode>();
        indexNode p = null;
        while (fis.available() > 0) {//判断文件是否已被读完
            p = (indexNode) ois.readObject();
            objArr.add(p);
        }
        ois.close();
        return objArr;
    }
    public static String bytesToString(byte[] bytes) {
        //转换成base64
        return org.apache.commons.codec.binary.Base64.encodeBase64String(bytes);
    }

    public double distance2(double[] x, double[] y) {
        double d = 0.0;
        for (int i = 0; i < x.length; i++) {
            d += (x[i] - y[i]) * (x[i] - y[i]);
        }
        return d;
    }
    public double distance(double[] x, double[] y) {
        return Math.sqrt(distance2(x, y));
    }



    static class ComparatorByRelaxIndexNode implements Comparator { //从大到小
        @Override
        public int compare(Object o1, Object o2){
            relaxIndexNode in1 = (relaxIndexNode)o1;
            relaxIndexNode in2 = (relaxIndexNode)o2;
            return (in2.getLb()-in1.getLb()>0)?1:-1; //降序

            //return o1-o2 升序
        }
    }
    static class ComparatorByRelaxIndexNodeReverse implements Comparator {
        @Override
        public int compare(Object o1, Object o2){
            relaxIndexNode in1 = (relaxIndexNode)o1;
            relaxIndexNode in2 = (relaxIndexNode)o2;
            return (in1.getLb()-in2.getLb()>0)?1:-1; //升序

        }
    }
}

