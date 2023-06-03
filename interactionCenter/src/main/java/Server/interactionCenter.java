package Server;

import Utils.indexAlgorithm;
import Utils.indexNode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class interactionCenter {
    ServerSocket server;
    distributedQuery dq;
    Socket socket;
    static List<Socket> list = new ArrayList<Socket>();//保存链接的Socket对象
    HashMap<String, Socket> socketHashMap = new HashMap<String, Socket>();
    HashMap<String, Integer> resultFrequency = new HashMap<>();
    public int id = 0;
    boolean isConstructedGlobalIndex = false;
    boolean onceQuery = false;

    long sendQueryTime = 0;
    long receiveResult = 0;
    long sendQueryByte = 0;
    long receiveByte = 0;

    long totalSearchTime = 0;
    long beginSearchTime = 0;

    public static int dimension = 2;
    public static String directory= "D:\\Github\\MSDS-code\\interactionCenter\\src\\Data\\Query\\";
    public HashMap<String, PriorityQueue<relaxIndexNode> > allQueryResults = new HashMap<>();
    public indexNode globalRootNode = new indexNode(dimension);
    public static ArrayList<String> queryList = new ArrayList<>();
    public Set<String> candiClientList;
    public static HashMap<String, Integer> resolutionHm = new HashMap<String, Integer>();
    public static int connectThreshold = 0;
    int clientSize = 1;
    public static int[] resolutionList = new int[]{10};
    public static int topk = 10;
    public static String searchType = "MIP";
    static String algorithm = "IBtree";
    public static String datasetName = "baidu"; //nyu-dataset
    public static String [] arr = new String[]{
            "上海_房地产_宿舍.csv",
//            "nyu_35000.csv"
    };


    static long globalIndexBuildTime = 0;
    ExecutorService exec;
    public interactionCenter(distributedQuery dq)  throws IOException, CloneNotSupportedException,ClassNotFoundException {
        this.dq = dq;
        for (int i = 0; i<arr.length; i++){
            queryList.add(arr[i]);
        }
        System.out.println("queryList.size() ="+queryList.size());
        try {
            server = new ServerSocket(22222);//启动监听，端口为22222
            while(true) {
                socket = server.accept();
                list.add(socket);
                exec = Executors.newCachedThreadPool();
                receive t = new receive(socket);
                t.setS("server receive = ");
                exec.execute(t);
                System.out.println("hm.size = " + socketHashMap.size()+", list.size = "+list.size());
            }

        } catch (IOException e) {
//            e.printStackTrace();
            System.out.println("failed 1");
        }
    }

    public synchronized void sendQuery(String canId, Socket socket, String queryName) throws IOException{
        //System.out.println("queryName  = "+queryName);
        dq.s.queryIndexNode = dq.queryListIndexNode.get(canId).get(queryName);
        dq.s.queryMap = dq.queryListMapSig.get(canId).get(queryName);
        dq.s.queryMapSignatureHashMap = dq.queryListMapSigHM.get(canId).get(queryName);

        //2,send query request
        String queryString = getQueryListString();
        send sr = new send(socket);
        String t = "query"+";"+dq.searchType+";"+dq.topk+";"+ queryString+";"+ algorithm+";"+ connectThreshold;
        System.out.println(t);
        System.out.println("t.getBytes().length = "+t.getBytes().length+" bytes"+",  t.size = "+t.length());
        sr.send(t);
        sendQueryByte += t.getBytes().length;


    }

    class receive implements Runnable {
        Socket socket;      //Socket object
        DataInputStream in; //input stream
        String s = "Server received the message: ";
        public receive(Socket socket) throws IOException{
            this.socket = socket;
            in = new DataInputStream(socket.getInputStream());
        }

        public void setS(String s){
            this.s = s;
        }

        public void run() {
            while (true) {
                try {
                    long receivetime1 = System.currentTimeMillis();
                    String receive = in.readUTF();  //read the message from the client
                    long receivetime2 = System.currentTimeMillis();
                    receiveByte += receive.getBytes().length;
                    receiveResult += (receivetime2 - receivetime1);
                    System.out.println("server receive = "+receive);
                    if (receive.startsWith("localIndex")) {
                        String[] s = receive.split(",");
                        socketHashMap.put(s[1], socket);
                        System.out.println("socketHashMap.size() = " + socketHashMap.size());
                        long startTime = System.currentTimeMillis();
                        if (s.length<10){
                            System.out.println("local index has error");
                        }else {
                            indexNode node = getlocalRootNode(receive);
                            indexNode rootKmeans = globalRootNode;
                            long endTime = System.currentTimeMillis();
                            addSibling(rootKmeans, node);
                            globalIndexBuildTime += (endTime-startTime); // index construction time
                            int resolution = Integer.parseInt(s[9]);
                            resolutionHm.put(s[1], resolution);
                            try {
                                resolution = interactionCenter.resolutionHm.get(s[1]);
                                dq.topKQuery(s[1], resolution);
                            }catch (Exception e ){
                                e.printStackTrace();
                            }
                        }

                        if (socketHashMap.size() >= clientSize){
                            beginSearchTime = System.currentTimeMillis();
//                            for (String s1: dq.queryListMapSig.keySet()){
//                                System.out.print("s1 = "+s1+": ");
//                                for (String s2: dq.queryListMapSig.get(s1).keySet())
//                                System.out.print(s2+", ");
//                            }
//                            System.out.println();

                            //1,construct the global index
                            System.out.println("global Index BuildTime = "+globalIndexBuildTime);
                            isConstructedGlobalIndex = true;
                            boolean hasCandi = true;

                            while (hasCandi){
                                String queryName = queryList.get(id);
                                System.out.println(queryName);
                            //2,find candidate dataset Set<Integer> getCandiClientList //socketHashMap.keySet()
//                            candiClientList = getCandiClientList(queryName); //
                          candiClientList = socketHashMap.keySet();
                          System.out.println("candiClientList.size() = " +candiClientList.size());
                            hasCandi = false;
                            if (candiClientList.size()>0){
                                System.out.print("canId = ");
                                for (String canId: candiClientList) {
                                System.out.print(canId+", ");
                                }
                                System.out.println();
                                //find candidate set and send query request
                                long sendQuery1 = System.currentTimeMillis();
                                for (String canId: candiClientList) {
                                    sendQuery(canId, socketHashMap.get(canId), queryName);
                                }
                                long sendQuery2 = System.currentTimeMillis();
                                sendQueryTime += (sendQuery2-sendQuery1);
                            }else {
                                System.out.println(queryName+"'s query result is null");
                                id++;
                            }
                            }
                        }
                    }else {
                        if (receive.startsWith("result")){
                            //result : deviceID : queryID : resultString
                            String[] v = receive.split(":");
//                            System.out.println("v.length = "+v.length);
                            synMethod(v);

                            if (onceQuery && id<queryList.size()-1){
                                //When one query ends, proceed to the next query, and set onceQuery = flase, id++
                                onceQuery = false;
                                id++;
                                String queryName = queryList.get(id);
                                //2,find candidate data source using global index
                                candiClientList = getCandiClientList(queryName);
                                //2,send to all data sources without global index
//                                candiClientList = socketHashMap.keySet();
                                System.out.println("candi.size = "+candiClientList.size());
                                //send query request to data source
                                System.out.print("canId = ");
                                for (String canId: candiClientList) {
                                    System.out.print(canId+", ");
                                }
                                System.out.println();

                                long sendQuery1 = System.currentTimeMillis();
                                for (String canId: candiClientList) {
                                    sendQuery(canId, socketHashMap.get(canId), queryName);
                                }
                                long sendQuery2 = System.currentTimeMillis();
                                sendQueryTime += (sendQuery2-sendQuery1);

                            }

//                            if (id >= dq.queryList.size()-1){
//                                System.out.println("sendQueryTime = "+sendQueryTime);
//                                System.out.println("receiveResult = "+receiveResult);
//                                for (int clientId: socketHashMap.keySet()){
//                                    send sr = new send(socketHashMap.get(clientId));
//                                    String t = "end";
//                                    sr.send(t);
//                                }
//                            }
                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("receive failed");
                    break;
                }
            }
        }
    }

    class send {
        Socket socket;      //服务器端Socket对象
        DataInputStream in; //数据输入输出流
        DataOutputStream ou;
        String s = "Server send to client";
        public send(Socket socket) throws IOException{
            this.socket = socket;
            ou = new DataOutputStream(socket.getOutputStream());
        }

        public void send(String s) {
            this.s = s;
            try {
                int maxTransfer = 50000;
                //new add
                if(s.length() > maxTransfer){
                    int part = (int)Math.ceil((double) s.length() / maxTransfer);
                    ou.writeUTF(String.valueOf(part));
                    ou.flush();
                    int mod = 0;
                    if (part * maxTransfer > s.length()) {
                        mod = s.length() % maxTransfer;
                    }
                    int count = 0;
                    while (count<part){
                        if (count == part - 1) {
                            String tempStr = s.substring(0,mod);
                            ou.writeUTF(tempStr);
                            ou.flush();
                        } else {
                            String tempStr = s.substring(0,maxTransfer);
                            s = s.substring(maxTransfer);
                            ou.writeUTF(tempStr);
                            ou.flush();
                        }
                        count++;
                    }
                }else{
                    ou.writeUTF(String.valueOf(1));
                    ou.flush();
                    ou.writeUTF(s);//向客户端发送查询信息
                    ou.flush();
                }
            } catch (Exception e) {
                    e.printStackTrace();
                System.out.println("send failed");

            }
        }
    }
//    }

    public static void main(String[] args)  throws IOException, CloneNotSupportedException,ClassNotFoundException {
        distributedQuery dq = new distributedQuery();
        new interactionCenter(dq);
    }

    public synchronized void synMethod(String[] v){
        String queryID = v[2];
//        System.out.println("1queryID ="+queryID);
        int frequency = resultFrequency.getOrDefault(queryID, 0);
//        System.out.println("1frequency = "+frequency);
        PriorityQueue<relaxIndexNode> allResults = allQueryResults.getOrDefault(queryID, new PriorityQueue<>(new ComparatorByRelaxIndexNode())); //small - large
        frequency++;
//        System.out.println("2frequency = "+frequency);
        resultFrequency.put(queryID, frequency);
        if (v.length>3){
            String resultString = v[3];
            String[] resultList = resultString.split(";");
            for(String s : resultList){
                String[] ss = s.split(",");
//                        if (Double.parseDouble(ss[1].trim()) > 0.0){
                relaxIndexNode re = new relaxIndexNode(ss[0], Double.parseDouble(ss[1].trim())); // , Double.parseDouble(deviceID)
                allResults.add(re);
            }
            allQueryResults.put(queryID, allResults);
        }else{
//            System.out.println("resultString = null");
            allQueryResults.put(queryID, allResults);
        }

        if (resultFrequency.get(queryID) >= candiClientList.size()){
            //output the result
            System.out.println("allQueryResults.get(queryID).size()() = "+allQueryResults.get(queryID).size());
            if (allQueryResults.get(queryID).size()>0){
                PriorityQueue<relaxIndexNode> p = allQueryResults.get(queryID);
                int t = 0;
                System.out.print(queryID+"'s "+"top-"+ dq.topk +" query result is=== ");
                while (!p.isEmpty() && t < dq.topk){
                    relaxIndexNode r = p.poll();
                    System.out.print(r.resultId+".csv, "+r.getLb()+"; ");
                    t++;
                }
                System.out.println();
            }
            onceQuery = true;
            //once query end
            try{
                if (id == queryList.size()-1){
                    System.out.println("sendQueryTime(ms) = "+sendQueryTime);
                    System.out.println("receiveResult(ms) = "+receiveResult);
                    System.out.println("sendBytes(bytes) = "+sendQueryByte);
                    System.out.println("receiveBytes(bytes) = "+receiveByte);
                    for (String clientId: socketHashMap.keySet()){
                        send sr = new send(socketHashMap.get(clientId));
                        String end = "end";
                        sr.send(end);
                    }
                    totalSearchTime = System.currentTimeMillis() - beginSearchTime;
                    System.out.println("totalSearchTime(ms) = "+ totalSearchTime);
                    socket.close();
                }
            }catch (IOException e){
                System.out.println("query has end");
            }
        }
    }

    public String getQueryListString(){
        String string = new String();
        string += dq.s.queryIndexNode.getNodeid()+","+ dq.s.queryIndexNode.getMBRmax()[0]+","+dq.s.queryIndexNode.getMBRmax()[1]+","+dq.s.queryIndexNode.getMBRmin()[0]+","+dq.s.queryIndexNode.getMBRmin()[1];
        String queryString="";
        String queryHistogramString="";
        for (int i =0; i<dq.s.queryMap.size(); i++){
            queryString += dq.s.queryMap.get(i)+",";
            if (dq.s.queryMapSignatureHashMap.get(dq.s.queryMap.get(i))!= null){
                queryHistogramString += dq.s.queryMapSignatureHashMap.get(dq.s.queryMap.get(i))+",";
            }else{
                queryHistogramString += 1+",";
            }

        }
        string = string +":" + queryString+":"+queryHistogramString;
        return string;
    }

    public double getOriCoordi(double coor, int reso, char x){
        double a = 0.0;
        double unitX = 360/Math.pow(2, reso);
        double unitY = 180/Math.pow(2, reso);
        switch (x){
            case 'x':
                a = coor*unitX - 180;
                break;
            case 'y':
                a = coor*unitY - 90;
                break;
        }
        return a;
    }

    public indexNode getlocalRootNode(String receive){
        String[] s = receive.split(",");
        indexNode localRootNode = new indexNode(interactionCenter.dimension);
        if (s.length<10){
            System.out.println("local index has error");
        }else {
            localRootNode.setDeviceID(s[1]);
            int reso = Integer.parseInt(s[9]);

            double[] MBRmax = {getOriCoordi( Double.parseDouble(s[2]), reso, 'x'), getOriCoordi( Double.parseDouble(s[3]), reso, 'y')};
            double[] MBRmin = {getOriCoordi( Double.parseDouble(s[4]), reso, 'x'), getOriCoordi( Double.parseDouble(s[5]), reso, 'y')};
            double[] pivot = {(MBRmax[0]+MBRmin[0])/2, (MBRmax[1]+MBRmin[1])/2};
            localRootNode.setMBRmax(MBRmax);
            localRootNode.setMBRmin(MBRmin);
            localRootNode.setPivot(pivot);
            localRootNode.setRadius(indexAlgorithm.distance(pivot, MBRmax));
            System.out.println(Double.parseDouble(s[2])+", "+Double.parseDouble(s[3])+", "+Double.parseDouble(s[4])+", "+Double.parseDouble(s[5]));

            System.out.println(MBRmax[0]+", "+MBRmax[1]+", "+MBRmin[0]+", "+MBRmin[1]);

        }
        return localRootNode;
    }

    public void addSibling (indexNode rootKmeans, indexNode node){
        if (rootKmeans.getNodelist().size() == 0){
            rootKmeans.setMBRmax(node.getMBRmax());
            rootKmeans.setMBRmin(node.getMBRmin());
            rootKmeans.setRadius(node.getRadius());
            rootKmeans.setPivot(node.getPivot());
            rootKmeans.addNodes(node);
            rootKmeans.setroot(-2);
//            rootKmeans = node;
        }else{
            rootKmeans.addNodes(node);
            Set<indexNode> nodeList = rootKmeans.getNodelist();
            double[] pivot = rootKmeans.getPivot();
            double radius = rootKmeans.getRadius();
            double rootMax[] = rootKmeans.getMBRmax();
            double rootMin[] = rootKmeans.getMBRmin();
            for (indexNode in : nodeList) {
                double nodeMax[] = in.getMBRmax();
                double nodeMin[] = in.getMBRmin();
                for (int dim = 0; dim < interactionCenter.dimension; dim++) {
                    if (nodeMax[dim] > rootMax[dim])
                        rootMax[dim] = nodeMax[dim];//need to revise, as the radius is small due to weight
                    if (nodeMin[dim] < rootMin[dim])
                        rootMin[dim] = nodeMin[dim];
                }
            }
            for(int dim = 0; dim< interactionCenter.dimension; dim++) {
                pivot[dim] = (rootMax[dim] + rootMin[dim])/2;
                radius += Math.pow((rootMax[dim] - rootMin[dim])/2, 2);
            }
            rootKmeans.setRadius(Math.sqrt(radius));
            rootKmeans.setPivot(pivot);
            rootKmeans.setMBRmax(rootMax);
            rootKmeans.setMBRmin(rootMin);
            rootKmeans.setroot(-2);
        }

    }

    public synchronized Set<String> getCandiClientList(String queryName){
        Set<String> candiClientList = new HashSet<>();
        Set<indexNode>localRootList = globalRootNode.getNodelist();
        //We need to convert the coordinates to latitude and longitude and then create a global index,
        for (indexNode in : localRootList) {
            dq.s.queryIndexNode = dq.queryListIndexNode.get(in.deviceID).get(queryName);
            indexNode a = new indexNode(dimension);
            double[] MBRmin = dq.s.queryIndexNode.getMBRmin();
            double[] MBRmax = dq.s.queryIndexNode.getMBRmax();
            System.out.println(MBRmax[0]+", "+MBRmax[1]+", "+MBRmin[0]+", "+MBRmin[1]);
            double[] newMBRmin = {getOriCoordi(MBRmin[0], resolutionHm.get(in.deviceID),'x')
            , getOriCoordi(MBRmin[1], resolutionHm.get(in.deviceID),'y')};
            double[] newMBRmax = {getOriCoordi(MBRmax[0], resolutionHm.get(in.deviceID),'x')
                    , getOriCoordi(MBRmax[1], resolutionHm.get(in.deviceID),'y')};
            double x = (newMBRmin[0]+newMBRmax[0])/2;
            double y = (newMBRmin[1]+newMBRmax[1])/2;
            double[] newPivot = {x,y};
            double newRadius = indexAlgorithm.distance(newPivot, newMBRmax);
            a.setMBRmin(newMBRmin);
            a.setMBRmax(newMBRmax);
            a.setPivot(newPivot);
            a.setRadius(newRadius);

            System.out.println(newMBRmax[0]+", "+newMBRmax[1]+", "+newMBRmin[0]+", "+newMBRmin[1]);

            if (searchType.equals("MIP")){
//                System.out.println("searchType = "+searchType);
                if(indexAlgorithm.intersected(in, a, dimension)){
                    candiClientList.add(in.deviceID);
                }
            }else if(searchType.equals("MCP")){
//                System.out.println("searchType = "+searchType);
                System.out.println(indexAlgorithm.distance(in.getPivot(), a.getPivot()) - in.getRadius() - a.getRadius());
                if (indexAlgorithm.distance(in.getPivot(), a.getPivot()) - in.getRadius() - a.getRadius() <= connectThreshold){
                    candiClientList.add(in.deviceID);
                }else {
                    System.out.println("No candidate silo");
                }
//
            }


        }

        return candiClientList;
    }


}

class ComparatorByRelaxIndexNode implements Comparator { //small->large
    public int compare(Object o1, Object o2){
        relaxIndexNode in1 = (relaxIndexNode)o1;
        relaxIndexNode in2 = (relaxIndexNode)o2;
        return (in2.getLb()-in1.getLb()>0)?1:-1;
    }
}
class ComparatorByRelaxIndexNodeReverse implements Comparator { //reverse
    public int compare(Object o1, Object o2){
        relaxIndexNode in1 = (relaxIndexNode)o1;
        relaxIndexNode in2 = (relaxIndexNode)o2;
        return (in1.getLb()-in2.getLb()>0)?1:-1;
    }
}

