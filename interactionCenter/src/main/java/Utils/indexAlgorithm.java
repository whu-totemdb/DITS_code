package Utils;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

public class indexAlgorithm {
    // setNodeid() represent the ID.csv of datasets
    public indexAlgorithm(){
        // init
    }

    public TreeMap<Integer, HashSet<Integer>> genInvertedIndex(){
        TreeMap<Integer, HashSet<Integer>> tm = new TreeMap<>();

        return tm;

    }

    /*
     * indexing the dataset using a kd-tree way
     */
    public static int count = 0;
    public static indexNode indexDatasetKD(HashMap<Integer, ArrayList<Long>> datasetIdMapSignature, HashMap<Integer, indexNode> datalakeRootNodes, indexNode parentNode, int dimension, int leafSize, int upperGlobalId, String siloID, boolean isBatchUpdate) {
        double minBox[] = new double[dimension];
        double maxBox[] = new double[dimension];
        for(int dim=0; dim<dimension; dim++) {
            minBox[dim] = Double.MAX_VALUE;
            maxBox[dim] = Double.MIN_VALUE;
        }
        // get the bounding box of all the nodes first,
        double pivot[] = new double[dimension];
        for(int i : datalakeRootNodes.keySet()) {
            indexNode aIndexNode = datalakeRootNodes.get(i);
            double nodeMax[] = aIndexNode.getMBRmax();
            double nodeMin[] = aIndexNode.getMBRmin();
            for(int dim=0; dim<dimension; dim++) {
                if(nodeMax[dim] > maxBox[dim])
                    maxBox[dim] = nodeMax[dim];//need to revise, as the radius is small due to weight
                if(nodeMin[dim] < minBox[dim])
                    minBox[dim] = nodeMin[dim];
                pivot[dim] += aIndexNode.getPivot()[dim]; //之前使用的是MBR的中心，现在用的是所有数据集pivot的中心
            }
        }

        // get the pivot point, and the range in multiple dimension, and the radius
        int d=0;
//        double maxrange = Double.MIN_VALUE;
        double radius = 0;
        for(int dim=0; dim<dimension; dim++) {
//            pivot[dim] = (maxBox[dim] + minBox[dim])/2;   //之前使用的是MBR的中心，现在用的是所有数据集pivot的中心，用MBR的中心会出现树的偏斜
            pivot[dim] = pivot[dim]/datalakeRootNodes.size();
//            radius += Math.pow((maxBox[dim] - minBox[dim])/2, 2);
        }
        radius = Math.max(distance(pivot,maxBox), distance(pivot,minBox));
        //create a new leaf node and return
        indexNode a = new indexNode(dimension);
        a.setRadius(Math.sqrt(radius));// the radius need to be bigger.
        count++;
//        System.out.println("maxBox = "+maxBox[0]+", "+ maxBox[1]);
//        System.out.println("minBox = "+minBox[0]+", "+ minBox[1]);
//        System.out.println("pivot = "+pivot[0]+", "+pivot[1]);
//        System.out.println("radius =  "+radius);

        a.setPivot(pivot);
        a.setMBRmax(maxBox);
        a.setMBRmin(minBox);
        a.setUpperGlobalID(upperGlobalId);
        a.setDeviceID(siloID);
        a.setParentNode(parentNode);
        if(datalakeRootNodes.size() <= leafSize) {
            HashMap<Integer, ArrayList<Long>> dataset = new HashMap<>();
            for(int i: datalakeRootNodes.keySet()) {
                indexNode root = datalakeRootNodes.get(i);
                ArrayList<Long> array = datasetIdMapSignature.get(i);
                dataset.put(i, array);
                //如果是批量传输，则不需要存储具体的节点，
                // 如果是每次一更新就传输，则需要传输具体的节点信息，因此这里加一个判断
                a.addNodes(root);
//                System.out.println("id = "+root.getNodeid()+", radius = " + root.radius);
                a.addNodeIds(root.getNodeid());
                root.setParentNode(a);
            }
            //a.rootToDataset = -1
            a.setroot(-1);//label it as leaf node
            // 生成 posting list
            TreeMap<Long, HashSet<Integer>> pl = genInvertedIndex(dataset);
            a.setPostinglist(pl);

            return a;
        }else {
                // dividing the space by the broadest dimension d
//                ArrayList<indexNode> rootleft = new ArrayList<indexNode>();
//                ArrayList<indexNode> rootright = new ArrayList<indexNode>();
                HashMap<Integer, indexNode> rootleft = new HashMap<>();
                HashMap<Integer, indexNode> rootright = new HashMap<>();
                d=0;
                for(int i: datalakeRootNodes.keySet()) {
                    indexNode aIndexNode = datalakeRootNodes.get(i);
                    if(aIndexNode.getPivot()[d] < pivot[d]) {
                        rootleft.put(i, aIndexNode);
                    }else {
                        rootright.put(i, aIndexNode);
                        }
                }
                if(rootleft.isEmpty() || rootright.isEmpty()) {
                    HashMap<Integer, ArrayList<Long>> dataset = new HashMap<>();
                    for(int i: datalakeRootNodes.keySet()) {
                        indexNode root = datalakeRootNodes.get(i);

                        ArrayList<Long> array = datasetIdMapSignature.get(i);
                        dataset.put(i, array);
//                        System.out.println("id = "+root.getNodeid()+", radius = " + root.radius);
                        a.addNodes(root);
                        a.addNodeIds(root.getNodeid());
                        root.setParentNode(a);

                    }
                    a.setroot(-1);//label it as leaf node
                    TreeMap<Long, HashSet<Integer>> pl = genInvertedIndex(dataset);
                    a.setPostinglist(pl);

                }else {
                    a.addNodes(indexDatasetKD(datasetIdMapSignature, rootleft, a, dimension, leafSize, 2*upperGlobalId, siloID, isBatchUpdate));
                    a.addNodes(indexDatasetKD(datasetIdMapSignature, rootright, a, dimension, leafSize, 2*upperGlobalId+1, siloID, isBatchUpdate));
                    a.setroot(-2);//lable it as internal node
                }
                return a;
        }
    }

    public static TreeMap<Long, HashSet<Integer>> genInvertedIndex(HashMap<Integer, ArrayList<Long>> dataset) {
        TreeMap<Long, HashSet<Integer>> postinglist = new TreeMap<>();
        for(int datasetid: dataset.keySet()) {
            ArrayList<Long> signatureArrayList = dataset.get(datasetid);
            for(long code: signatureArrayList) {
                HashSet<Integer> datasetList;
                if(postinglist.containsKey(code))
                    datasetList = postinglist.get(code);
                else
                    datasetList = new HashSet<>();
                datasetList.add(datasetid);
                postinglist.put(code, datasetList);
            }

        }
        return postinglist;
    }

    //1,为每个数据集创建RootsDataset
    public static void createDatasetIndex(HashMap<String, indexNode> datalakeRootNodes, ArrayList<indexNode> indexNodes, String a, int dimension, double[][] dataset) {
        indexNode rootBall;
       // just create the root node, for datalake creation
        rootBall = createRootsDataset(dataset, dimension);
        rootBall.setNodeid(a);// set an id to identify which dataset it belongs to
        indexNodes.add(rootBall);
        datalakeRootNodes.put(a, rootBall);
    }

    public static indexNode createRootsDataset(double[][] dataset, int dimension) {
        double max[], min[], pivot[], radius=0;
        max = new double[dimension];
        min = new double[dimension];
        for(int i=0; i<dimension;i++) {
            max[i] = Double.MIN_VALUE;
            min[i] = Double.MAX_VALUE;
        }
        pivot = new double[dimension];
        for(double []datapoint: dataset) {
            for(int i=0; i<dimension;i++) {
                if(datapoint[i]>max[i])
                    max[i] = datapoint[i];
                if(datapoint[i]<min[i])
                    min[i] = datapoint[i];
                pivot[i] += datapoint[i];
            }
        }
        for(int i=0; i<dimension;i++) {
//            pivot[i] = (max[i]+min[i])/2;
            radius += Math.pow((max[i] - min[i])/2, 2);
        }
        for (int i=0; i<dimension; i++){
            pivot[i] = pivot[i]/dataset.length;
        }
        radius = Math.sqrt(radius);
        indexNode rootNode = new indexNode(dimension);
        rootNode.setMBRmax(max);
        rootNode.setMBRmin(min);
        rootNode.setRadius(radius);
        rootNode.setPivot(pivot);
//        rootNode.setTotalCoveredPoints(dataset.length);
        return rootNode;
    }

    public static void reverseFindParent(indexNode parentIndexNode, indexNode childIndexNode, int dimension){

        //判断子节点的MBR是否包含在父节点里面
        double[] minMBRofParent = parentIndexNode.getMBRmin();
        double[] maxMBRofParent = parentIndexNode.getMBRmax();
        double[] minMBRofChild = childIndexNode.getMBRmin();
        double[] maxMBRofChild = childIndexNode.getMBRmax();

        for (int i = 0; i<dimension; i++){
            //1,如果改变的数据集节点包括在父节点的MBR里，则不再反向搜索
            if (minMBRofChild[i] <= minMBRofParent[i] && maxMBRofChild[i] <= maxMBRofParent[i]){

            }else{
                //2,如果改变的数据集节点不包括在父节点的MBR里，则修改节点的MBR并继续反向搜索
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
                double radius = Math.max(DataLoader.distance2(parentIndexNode.getPivot(), minBox),DataLoader.distance2(parentIndexNode.getPivot(), maxBox));
                parentIndexNode.setRadius(radius);
                //直到根节点为止
                if (parentIndexNode.getParentNode() != null){
                    reverseFindParent(parentIndexNode.getParentNode(), parentIndexNode, dimension);
                }

            }
        }

    }





    public void storeDatalakeIndex(indexNode root, String nodeid, String storePath) {
        // assign an id to the node
        String contentString = Integer.toString(root.getUpperGlobalID()) + "," +  nodeid + "," + Double.toString(root.getRadius());
        if (root.getDatasetID() == -1) {  // leaf node
            contentString += ",1";
        } else if (root.getDatasetID() == -2){ // internal node
            contentString += ",2";
        } else {
            contentString += ",0";
        }
        contentString += ":";// store the pivot
        double[] pivot = root.getPivot();
        for (double a : pivot)
            contentString += Double.toString(a) + ",";
        contentString += ":";
        for (double max : root.getMBRmax()) //add the max and min bounding box
            contentString += Double.toString(max) + ",";
        contentString += ":";
        for (double min : root.getMBRmin())
            contentString += Double.toString(min) + ",";
        contentString +=":";

        if (root.getDatasetID() == -1) {  // leaf node
            for (String i: root.getNodeIDList()){
                contentString = contentString+ i+",";
            }
        } else { // internal node
            for (indexNode in: root.getNodelist()){
                contentString = contentString+ String.valueOf(in.getUpperGlobalID()) +",";
            }
        }
        write(storePath, contentString + "\n");

        if (root.getDatasetID() < 0) {
            Set<indexNode> listnode = root.getNodelist();
            for (indexNode aIndexNode : listnode) {
                storeDatalakeIndex(aIndexNode, aIndexNode.getNodeid(), storePath);
            }
        }
    }

    public static void write(String fileName, String content) {
        RandomAccessFile randomFile = null;

        try {
            randomFile = new RandomAccessFile(fileName, "rw");
            long fileLength = randomFile.length();
            randomFile.seek(fileLength);
            randomFile.writeBytes(content);
        } catch (IOException var13) {
            var13.printStackTrace();
        } finally {
            if (randomFile != null) {
                try {
                    randomFile.close();
                } catch (IOException var12) {
                    var12.printStackTrace();
                }
            }

        }

    }

    public static boolean intersected(indexNode root, indexNode query, int dim) {
        double[] mbrmin = root.getMBRmin();
        double[] mbrmax = root.getMBRmax();
//        System.out.println("in.MBR"+ mbrmax[0]+", "+mbrmax[1]+", "+mbrmin[0]+", "+mbrmin[1]);
        double[] querymin = query.getMBRmin();
        double[] querymax = query.getMBRmax();
//        System.out.println("query.MBR"+ querymax[0]+", "+querymax[1]+", "+querymin[0]+", "+querymin[1]);

        for(int i=0; i<dim; i++)
            if(mbrmin[i] > querymax[i] || mbrmax[i] < querymin[i]) {
                return false;
            }
        return true;
    }

    public static boolean MCPintersected(indexNode root, indexNode query, int dim) {
        double[] mbrmin = root.getMBRmin();
        double[] mbrmax = root.getMBRmax();
        double[] querymin = query.getMBRmin();
        double[] querymax = query.getMBRmax();
        for(int i=0; i<dim; i++)
            if(mbrmin[i] > querymax[i] || mbrmax[i] < querymin[i]) {
                return false;
            }
        return true;
    }


    public static double distance2(double[] x, double[] y) {
        double d = 0.0;
        for (int i = 0; i < x.length; i++) {
            d += (x[i] - y[i]) * (x[i] - y[i]);
        }
        return d;
    }
    public static double distance(double[] x, double[] y) {
        return Math.sqrt(distance2(x, y));
    }

}
