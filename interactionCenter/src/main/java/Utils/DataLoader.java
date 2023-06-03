package Utils;

import Server.Server;

import java.io.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

public class DataLoader {

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

    public static void writeFileAndMBR(HashMap<Integer, double[][]> zcodeMBR, String filePath, HashMap<Integer, HashMap<Long,Double>> dataSetMap) throws IOException {
        FileWriter fw = null;
        try {
            File file = new File(filePath);
            file.createNewFile();
            fw = new FileWriter(filePath);
            for (int id : dataSetMap.keySet()) {
                HashMap<Long,Double> map1 = dataSetMap.get(id);
                fw.write(String.valueOf(id)+",");
                double[][] MBRs = zcodeMBR.get(id);
                fw.write(MBRs[0][0]+",");
                fw.write(MBRs[0][1]+",");
                fw.write(MBRs[1][0]+",");
                fw.write(MBRs[1][1]+";");
//                fw.write(";");
                for (long key: map1.keySet()){
                    fw.write(String.valueOf(key));
                    fw.write(",");
                    String aa = String.valueOf(map1.get(key));
                    BigDecimal db = new BigDecimal(aa);
                    String ii = db.toPlainString();
                    fw.write(ii+";");
                }
                fw.write("\n");

            }
            fw.close();
            System.out.println("resolution.txt write successfully");

        }catch(Exception e){
            e.printStackTrace();
        }
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

            }
            fw.close();

        }catch(Exception e){
                e.printStackTrace();
            }
    }

    public static TreeMap<Long,Double> generateSignature(double minX, double minY, double rangeX, double rangeY, int resolution, String Path, String querySilo) throws CloneNotSupportedException{
        double blockSizeX = rangeX/Math.pow(2,resolution);
        double blockSizeY = rangeY/Math.pow(2,resolution);
        TreeMap<Long,Double> ZorderDensityHist = new TreeMap<>();
        long t = (long)Math.pow(2,resolution);
        int a,b;
        if (querySilo.equals("argoverse")){
            a = 6; //经纬度
            b = 7;
        }else if (querySilo.equals("baidu")){
            a = 1; //lng
            b = 2; //lat
        }else{
            a = 0;
            b = 1;
        }
        try {
            String record = "";
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(Path)));
            reader.readLine();
            while((record=reader.readLine())!=null){
//                System.out.println(record);
                String[] fields = record.split(",");
                double x_coordinate = Double.parseDouble(fields[a]);
                double y_coordinate = Double.parseDouble(fields[b]);
                if (fields[a].matches("-?\\d+(\\.\\d+)?") && x_coordinate>-180 && x_coordinate < 180 &&
                        y_coordinate> -90 && y_coordinate <90) {
                    double x = Double.parseDouble(fields[a]) - minX;
                    double y = Double.parseDouble(fields[b]) - minY;
                    long X = (long) (x / blockSizeX);  //行row
                    long Y = (long) (y / blockSizeY);  //列 col
                    long id = Y * t + X; //Z-order code
                    Double num = ZorderDensityHist.getOrDefault(id, new Double(0));
//                ZorderHis.put(id,num+1);
                    ZorderDensityHist.put(id, (double) (num + 1));
                }
            }
            reader.close();
        }catch (IOException e) {
            //数据集名字不连贯，如果有不存在的csv就可以跳过
//            e.printStackTrace();
//            System.out.println("this dataset name isn't exist");
        }
        return ZorderDensityHist;
    }

    public static indexNode generateQuery(String dataPath, ArrayList<Long> arrayList, TreeMap<Long,Double> hashMap, String queryID, int dimension, int resolution, double minX,double minY,double rangeX, double rangeY, String querySilo) throws IOException,CloneNotSupportedException{
       //读查询数据集
        TreeMap<Long, Double> hashMap1= generateSignature(minX, minY, rangeX, rangeY, resolution, dataPath, querySilo);
        hashMap = (TreeMap<Long, Double>)hashMap1.clone();
        double[][] dataset = new double[hashMap.size()][dimension];
        int count = 0;
//        System.out.println("!!queryID = "+queryID+", "+"size = "+hashMap.size()+"; ");
        for (long j: hashMap.keySet()){
            double[] d = DataLoader.resolve(j,resolution); // get coordinate
            dataset[count] = d;
            count++;
            arrayList.add(j);
        }
        indexNode queryIndexNode = indexAlgorithm.createRootsDataset(dataset, dimension);
        queryIndexNode.setNodeid(queryID);
        return queryIndexNode;
    }

    public static void generateQueryWithSever(String dataPath, Server s, int queryID, int dimension, int resolution, double minX, double minY, double rangeX, double rangeY, String querySilo) throws IOException,CloneNotSupportedException{
        //读查询数据集
        s.queryMapSignatureHashMap= generateSignature(minX, minY, rangeX, rangeY, resolution, dataPath, querySilo);
        double[][] dataset = new double[s.queryMapSignatureHashMap.size()][dimension];
        int count = 0;
        System.out.println("queryID = "+queryID+".csv  "+"query.size() = "+ s.queryMapSignatureHashMap.size());
//        System.out.print("Query"+" = ");
        for (long j: s.queryMapSignatureHashMap.keySet()){
            double[] d = DataLoader.resolve(j,resolution); // get coordinate
//            System.out.print(j+": "+d[0]+", "+d[1]+", "+hashMap.get(j)+"; ");
            dataset[count] = d;
            count++;
            s.queryMap.add(j);
        }
//        System.out.println();
        s.queryIndexNode = indexAlgorithm.createRootsDataset(dataset, dimension);
    }


    public static double distance2(double[] x, double[] y) {
        double d = 0.0;
        for (int i = 0; i < x.length; i++) {
            d += (x[i] - y[i]) * (x[i] - y[i]);
        }
        return d;
    }
    public double distance(double[] x, double[] y) {
        return Math.sqrt(distance2(x, y));
    }


}
