package Utils;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

    public class indexNode implements Serializable,Cloneable {
    private static final long serialVersionUID = 1L;
    public Set<Integer> pointIdList; // the leaf node, the index node is a leaf node when this is not empty, we can
    public Set<indexNode> nodeList; // the internal node
    public indexNode parentNode; //parent node
    public Set<String> nodeIDList;//check whether the root is empty before use, for seralization
    protected double pivot[];// the mean value
    protected transient double radius;// the radius from the pivot to the furthest point
    public transient int upperGlobalID;
    public String deviceID;
    double []mbrmax;//MBR bounding box
    double []mbrmin;

    public TreeMap<Long, HashSet<String>> postinglist;

    public int length;

    String nodeid = "0";//for node identification, dataset id
    int rootToDataset = 0;// -1 indicts leaf node, -2 indicts internal node

    public indexNode(int dimension) {
        pointIdList = new HashSet<>();//??ball?????id
        nodeList = new HashSet<>(); // indexNode???HashSet,?????internal node
        pivot = new double[dimension]; //pivot??
        mbrmax = new double[dimension];// only for dataset search ?????
        mbrmin = new double[dimension]; // only for dataset search ?????
        radius = Double.MAX_VALUE; // this is for the root node ??
    }
    public void setDeviceID(String id){
        this.deviceID = id;
    }

    public String getDeviceID(){
        return this.deviceID;
    }

    public void setParentNode(indexNode parent){
        parentNode = parent;
    }

    public indexNode getParentNode(){
        return parentNode;
    }

    public void setMBRmin(double[] mbrmin) {
        this.mbrmin = new double[mbrmin.length];
        for(int i=0; i<mbrmin.length; i++)
            this.mbrmin[i] = mbrmin[i];
    }

    public void setMBRmax(double[] MBRMAX) {
        this.mbrmax = new double[mbrmax.length];
        for(int i=0; i < MBRMAX.length; i++) {
            this.mbrmax[i] = MBRMAX[i];
        }

    }


    public double[] getMBRmin() {
        return mbrmin;
    }

    public double[] getMBRmax() {
        return mbrmax;
    }

    public void setNodeid(String nodeid) {
        this.nodeid = nodeid;
    }

    public String getNodeid() {
        return nodeid;
    }

    public void setRadius(double radius) {
        this.radius = radius;
    }

    public void setPivot(double pivot[]) {
        for(int i=0; i<pivot.length; i++)
            this.pivot[i] = pivot[i];
    }

    public void setUpperGlobalID(int id){
        this.upperGlobalID = id;
    }

    public int getUpperGlobalID(){
        return this.upperGlobalID;
    }

    public void addNodes(indexNode newNode) {
        nodeList.add(newNode);
    }

    public void addNodeIds(String nodeid) {
        if(nodeIDList==null)
            nodeIDList = new HashSet<>();
        nodeIDList.add(nodeid);
    }

    public void deleteNodeIds(int nodeid){
        nodeIDList.remove(nodeid);
    }

    public void setroot(int datasetID) {
        this.rootToDataset = datasetID;
    }
    //get rootToDataset
    public int getroot() {
        return rootToDataset;
    }

    public double[] getPivot() {
        return pivot;
    }

    public Set<indexNode> getNodelist() {
        return nodeList;
    }

    //get rootToDataset
    public int getDatasetID() {
        return rootToDataset;
    }

    public Set<String> getNodeIDList() {return nodeIDList;}

    public double getRadius() {
        return radius;
    }

    public void clearNodes() {
        nodeList = new HashSet<indexNode>();
    }

    public void removeNode(indexNode tempIndexNode) {
        nodeList.remove(tempIndexNode);
    }

    public void setPostinglist(TreeMap<Long, HashSet<String>> pl){
        postinglist = new TreeMap<>();
        postinglist = pl;
    }
    public TreeMap<Long, HashSet<String>> getPostinglist(){
        return postinglist;
    }


    @Override
    public indexNode clone() throws CloneNotSupportedException {
        // 实现拷贝--再拷贝一个Car赋值给Sudent
        indexNode cloneStudent = (indexNode) super.clone();
//        Car cloneCar = (Car) this.car.clone();
//        cloneStudent.setCar(cloneCar);
        return cloneStudent;
    }

}
