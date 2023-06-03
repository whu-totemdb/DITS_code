package Client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class dataSource {
    static ExecutorService exec = Executors.newCachedThreadPool();
    Silo silo;
    int dimension = 2;
    static int capacity = 10;
    double minX = -180;
    double minY = -90;
    double rangeX = 360;
    double rangeY = 180;
    public int[] resoList = {12}; //, 14, 15, 16
    int resolution = 10;
    int indexNodeTime = 0;
    int topk = 1;
    static String searchType =  "MIP";  // "MCP"; //
    static String algo = "IBtree"; // "IBtree", "quadTree", "RTree", "josie", "STS3"
    static String IPaddress = "10.128.128.31";
    static boolean loadDatasetGraph = false;
    String siloID = "1";
    public static String siloName = "baidu";
    public static String directory= "D:\\datasets\\MultiSource\\";
    public static String preprocessFilePath = "D:\\GitHub\\MSDS-dataSource\\src\\Data\\" ;
    public static String storeIndexPath = "D:\\GitHub\\MSDS-dataSource\\src\\Data\\";
    public static String graphfilePath;
    public dataSource() throws IOException, CloneNotSupportedException, ClassNotFoundException {
        //1,data preprocessing
        for (int ii =0 ; ii< resoList.length; ii++) {
            resolution = resoList[ii];
            System.out.println("siloName = "+siloName+",  "+ "resolution = "+resolution);
            silo = new Silo(siloID, siloName, dimension, capacity, minX, minY, rangeX, rangeY, resolution, algo);
            graphfilePath = preprocessFilePath + siloName+"\\" + "graph-" +"-reso" + resolution +".txt";
            indexNodeTime += silo.indexCheckinData(directory+ siloName+"/");
        }
        System.out.println("indexCheckinData complete");
        try {
            Socket socket = new Socket(IPaddress, 22222);
            //send information to server
            System.out.println(socket+", "+socket.getLocalAddress());
            //3, transit the root node
            send firstConnect = new send(socket);
            String localIndex = silo.getRootNode();
            firstConnect.send(localIndex);
            //5，receive the query request
            receive secondConnect = new receive(socket);
            exec.execute(secondConnect);

            //socket只要连接就不能close,因为需要重复通信，只能关闭线程
//            send sendResult = new send(socket);
//            sendResult.setS("result = ");
//            exec.submit(sendResult);
//
//            exec.shutdown();
//
//            //receive information
//            receiveAnswer r = new receiveAnswer(socket);
//            r.setS("receive information from server");
//            exec.execute(r);

        } catch (Exception e) {
//            e.printStackTrace();
        }


    }

    class send{
        Socket socket;   //client object
        DataInputStream in;
        DataOutputStream ou;
        String s = "hello";
        public send( Socket s){
            socket = s;
        }

        public void send(String s) {
            this.s = s;
            try {
                ou = new DataOutputStream(socket.getOutputStream());
                ou.writeUTF(s);  //send information to Server
                System.out.println("client send = " + s);
                ou.flush();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    class receive implements Runnable {

        Socket socket;
        DataInputStream in;

        public receive(Socket s){
            socket = s;
        }

        String s = " hello ";
        public void setS(String s){
            this.s = s;
        }

        public void run() {
            while (true) {
                StringBuffer ret = new StringBuffer("");
                try {
                    in = new DataInputStream(socket.getInputStream());//获取Socket中的输入输出流，以完成通信
                    int partCount = Integer.valueOf(in.readUTF()); //第一次接受获取服务器端需要传输的次数
                    for (int i = 0; i < partCount; i++) {
                        ret.append(in.readUTF());
                    }

                    String receive = ret.toString();

                    System.out.println("client receive: " + receive);

                    if (receive.startsWith("query")) {
                        String[] s = receive.split(";");
                        searchType = s[1];
                        topk = Integer.valueOf(s[2]);
                        silo.topK = Integer.valueOf(s[2]);
                        System.out.println("searchType = "+ searchType+", "+"topk = "+topk);
//
                        silo.getQueryFromResponse(s[3]); //产生queryNode
                        silo.algorithm = s[4];
                        algo = s[4];
                        silo.connectedThreshold = Integer.parseInt(s[5]);

                        PriorityQueue<relaxIndexNode> result = new PriorityQueue<>(new Comparator<relaxIndexNode>() {
                            @Override
                            public int compare(relaxIndexNode o1, relaxIndexNode o2) {
                                return (o1.getLb()-o2.getLb()>0)?1:-1;
                            }
                        }); //small - large
                        if(searchType.equals("MIP") ){
                            System.out.println("searchType.equals(\"MIP\") = "+searchType.equals("MIP"));
                            result = silo.topKQuery(silo.queryId);
                        }else if (searchType.equals("MCP")){
                            System.out.println("algo = "+algo);
                            switch (algo){
                                case "IBtree":
                                    // GADM algorithm
                                    result = silo.greedyMcpGraph();
                                    break;
                                case "sliceSearch":
                                    // GASM algorithm
                                    result = silo.greedyMcpOnlytree();
                                    break;
                                case "MCPbaseline":
//                                   //SG algorithm
                                    result = silo.standardGreedyMcpWithoutIndex(); // withou tree index
                            }
                        }else {
                            System.out.println("search type have error!!!!!!!!!!!");
                        }
//                        silo.deviceResultList.put(silo.queryId, result);


                        //发送查询结果给server
                        String d_result = "";
                        while (result.size() > 0){ // && count<topk !result.isEmpty()
                            relaxIndexNode r= result.poll();
                            if (r.getLb() > 0.0){
                                d_result += r.resultId+","+r.getLb()+";"; //d.datasetIDList.get(d.histogram_name[r.resultId-1])是.csv文件名
                            }
                        }
                        String re  = "result"+":" + silo.siloID+ ":" + silo.queryId + ":" + d_result;
//                        if (!d_result.isEmpty()){
                            send sendResult = new send(socket);
                            sendResult.send(re);
//                        }
                    }
                    if (receive.startsWith("end")){
                        socket.close();
                    }

                } catch (Exception e) {
//                e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, CloneNotSupportedException, ClassNotFoundException {
        //1,client send local index , server answer ok
        //2, server received information and construct the global index
        //3, server send query information to specific client and client implement query
        //4, client send the result
        new dataSource();

    }
}



