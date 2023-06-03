package Server;

import Utils.DataLoader;
import Utils.indexNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

public class distributedQuery {
    public String [] arr = interactionCenter.arr;

    public static  String directory= interactionCenter.directory;
    public static String datasetName = interactionCenter.datasetName;
    public int t = 0;
    public  String searchType = interactionCenter.searchType;     //     MCP: // String[] algos = new String[]{ "MCPbaseline", "sliceSearch",  "IBtree"};
    public int[] resolutionList = interactionCenter.resolutionList; //10, 11, 12, 13, 14
    public int resolution;
    public int topk = interactionCenter.topk;
    public static HashMap<String, ArrayList<Long>> subqueryListMapSig = new HashMap<>();
    public static HashMap<String, TreeMap<Long,Double>> subqueryListMapSigHM = new HashMap<>();
    public static HashMap<String, indexNode> subqueryListIndexNode = new HashMap<>();

    public static HashMap<String, HashMap<String, ArrayList<Long>>> queryListMapSig = new HashMap<>();
    public static HashMap<String, HashMap<String, TreeMap<Long,Double>>> queryListMapSigHM = new HashMap<>();
    public static HashMap<String, HashMap<String, indexNode>> queryListIndexNode = new HashMap<>();

    public static Server s;


    public distributedQuery(){
    }

    public void topKQuery() throws IOException,CloneNotSupportedException, ClassNotFoundException{
        s = new Server();
        for (int ii = 0; ii<resolutionList.length; ii++){
            resolution =  resolutionList[ii];
            System.out.println("resolution = "+resolution +", topk = "+topk);
            int dimension = 2;
            double minX =-180;
            double minY =-90;
            double rangeX = 360;
            double rangeY = 180;

            String queryName;
            String algorithm;
            System.out.println("======================");
            int batch = 1000;
            int num = 20;


//            File f = new File(directory);
//            File[] ff = f.listFiles();
            //queryList.size() //ff.length
            for (int i = t; i< interactionCenter.queryList.size() ; i++){
                queryName = interactionCenter.queryList.get(i);
                //1, 产生query
//                System.out.println("i = "+i);
//                queryName = ff[i].getName();
                String queryPath = directory +queryName;
//                System.out.println(queryPath);
//                String queryPath = directory+ datasetName +"/"+queryName;
                System.out.print("queryName = "+queryName);
                ArrayList<Long> queryMapSignature = new ArrayList<>();
                TreeMap<Long,Double> queryMapSignatureHashMap = new TreeMap<Long,Double>();

                indexNode queryIndexNode =
                        DataLoader.generateQuery(queryPath, queryMapSignature, queryMapSignatureHashMap, queryName, dimension, resolution, minX, minY, rangeX, rangeY, datasetName);

                subqueryListMapSig.put(queryName, queryMapSignature);
                subqueryListIndexNode.put(queryName, queryIndexNode);
                subqueryListMapSigHM.put(queryName, queryMapSignatureHashMap);
            }

        }


    }

    public void topKQuery(String siloId, int resolution) throws IOException,CloneNotSupportedException, ClassNotFoundException{
        s = new Server();
        System.out.println("siloId = "+ siloId + ", " + "resolution = " + resolution + ", topk = " + topk);
        int dimension = 2;
        double minX = -180;
        double minY = -90;
        double rangeX = 360;
        double rangeY = 180;
        String queryName;
        int batch = 1000;
        int num = 20;
        subqueryListMapSig.clear();
        subqueryListIndexNode.clear();
        subqueryListMapSigHM.clear();


        for (int i = t; i < interactionCenter.queryList.size(); i++) {
            queryName = interactionCenter.queryList.get(i);
            String queryPath = directory  + queryName;
            ArrayList<Long> queryMapSignature = new ArrayList<>();
            TreeMap<Long, Double> queryMapSignatureHashMap = new TreeMap<Long, Double>();
            indexNode queryIndexNode =
                    DataLoader.generateQuery(queryPath, queryMapSignature, queryMapSignatureHashMap, queryName, dimension, resolution, minX, minY, rangeX, rangeY, datasetName);
            subqueryListMapSig.put(queryName, queryMapSignature);
            subqueryListIndexNode.put(queryName, queryIndexNode);
            subqueryListMapSigHM.put(queryName, queryMapSignatureHashMap);
        }

        s.clear();
        queryListMapSig.put(siloId, subqueryListMapSig);
        s.dimension = dimension;
        queryListIndexNode.put(siloId, subqueryListIndexNode);
        queryListMapSigHM.put(siloId, subqueryListMapSigHM);

    }

    public static ArrayList<Integer> randomGenerateQuery(int min, int max){
        ArrayList<Integer> arrayList = new ArrayList<>();
        for (int i = min-1; i < max-1; i++){
            arrayList.add(i+1);
        }
        return arrayList;
    }


}
