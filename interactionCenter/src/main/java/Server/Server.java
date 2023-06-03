package Server;

import Utils.indexAlgorithm;
import Utils.indexNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

public class Server {
    public int dimension = 2;
    public int resolution = 12;
    public double minX = -180; //-120;
    public double minY = -90; //500;
    public double rangeX = 360;
    public double rangeY = 180; //5120;
    public indexNode globalRootNode; //树的根节点
    public indexAlgorithm indexDSS = new indexAlgorithm();
    public ArrayList<Long> queryMap = new ArrayList<>();
    public indexNode queryIndexNode;

    public HashMap<String, HashMap<String, ArrayList<Long>>> queryListMap = new HashMap<>();
    public HashMap<String, HashMap<String, indexNode>> queryListIndexNode = new HashMap<>();
    public HashMap<String, HashMap<String, TreeMap<Long,Double>>> queryListMapSigHM = new HashMap<>();

    public TreeMap<Long,Double> queryMapSignatureHashMap = new TreeMap<>(); //由小到大

    public Server(){

    }
    public Server(HashMap<String, HashMap<String, ArrayList<Long>>> a, int d, HashMap<String, HashMap<String, indexNode>> queryIndexNode, HashMap<String, HashMap<String, TreeMap<Long,Double>>> b){
        queryListMap = a;
        queryListIndexNode = queryIndexNode;
        queryListMapSigHM =b;
        dimension = d;
        this.globalRootNode = new indexNode(dimension);
    }

    public indexNode getGlobalGridIndex(){
        return globalRootNode;
    }

    public void clear(){
        this.dimension = 2;
        this.resolution = 16;
        this.minX = -180;
        this.minY = -90;
        this.rangeX = 360;
        this.rangeY = 180;
        this.globalRootNode = new indexNode(dimension); //树的根节点
        this.indexDSS = new indexAlgorithm();
        this.queryMap = new ArrayList<>();
        this.queryIndexNode = new indexNode(dimension);
        this.queryListMap = new HashMap<>();
        this.queryListIndexNode = new HashMap<>();
        this.queryListMapSigHM = new HashMap<>();
        this.queryMapSignatureHashMap = new TreeMap<>(); //由小到大
    }




    }
