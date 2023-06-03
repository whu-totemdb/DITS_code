package Utils;

import Client.Silo;
import Client.relaxIndexNode;

import java.util.*;

public class Josie {
    public static TreeMap<Long, TreeMap<String, int[]>> createJosiePostinglists(HashMap<String, ArrayList<Long>> dataset) {
        TreeMap<Long, TreeMap<String, int[]>> postlisitngHashMap = new TreeMap<Long, TreeMap<String, int[]>>();
        //1, 先给每个数据集ID进行排序
        //2, 记录 (ID，position，total)
        for(String datasetid: dataset.keySet()) {
            ArrayList<Long> signatureArrayList = dataset.get(datasetid);
            int totalSize = signatureArrayList.size();
            ArrayList<Long> sortsignatureArrayList = sortArray(signatureArrayList);
            for (int i = 0; i<sortsignatureArrayList.size(); i++){
                long token = sortsignatureArrayList.get(i);
                TreeMap<String, int[]> entryOfPosting;
                int[] posting = new int[2];
                if(postlisitngHashMap.containsKey(token)){
                    entryOfPosting = postlisitngHashMap.get(token);
                }else{
                    entryOfPosting = new TreeMap<>();
                }
                    posting[0] = i+1;
                    posting[1] = totalSize;
                    entryOfPosting.put(datasetid, posting);

                postlisitngHashMap.put(token,entryOfPosting);
            }
        }
        return postlisitngHashMap;
    }

    public static ArrayList<Long> sortArray(ArrayList<Long> Array){
        PriorityQueue<Long> sortPQ = new PriorityQueue<>();
        ArrayList<Long> sortsignatureArrayList = new ArrayList<>();
        for (long code: Array){
            sortPQ.add(code);
        }
        while (!sortPQ.isEmpty()){
            sortsignatureArrayList.add(sortPQ.poll());
        }
        return sortsignatureArrayList;
    }

    public static double NetCostB(ArrayList<Long> X, int i, int i1, int qJoinXk, HashMap<Long, ArrayList<int[]>> InvertedIndex,int datasetId,  int[] positionList, ArrayList<Long> Query, int p, HashMap<Integer, int[]> U, HashMap<Integer, Integer> Ix0, int queryID){
        double cost = 0.0;
        int L = 0;

        for (int t = i+1; t<i1; t++){
//            System.out.println("NetCostB t = "+t);
            Long queryId = Query.get(t);
            for (int[] array:InvertedIndex.get(queryId)){
                L += array[2];
            }
        }
        int Q_Join_X_est = (positionList[3]/(Query.size()-Ix0.get(datasetId)+1))*(i1-i);
        int Jx_est = positionList[1]+ (i1-i)*(X.size()-positionList[0]+1)/(Query.size()-Ix0.get(datasetId)+1);
        int Q_Join_X_ub_est = positionList[3]+ Q_Join_X_est + Math.min(Query.size()-i1, X.size()-Jx_est);

        int benefit = 0;
        for (int id: U.keySet()){
            int[] YpositionList = U.get(id);
            if (Q_Join_X_ub_est <= qJoinXk){
                benefit += YpositionList[2] - YpositionList[1];
            }else {
                benefit += Jx_est - YpositionList[1];
            }
        }
        cost = L - benefit;
        return  cost;
    }
    public static double NetCost(int qJoinXk_1, int qJoinXk, HashMap<Long, ArrayList<int[]>> InvertedIndex, int i, String datasetId, int[] positionList, ArrayList<Long> Query, int p, HashMap<Integer, int[]> U, HashMap<Integer, Integer> Ix0, int queryID){
        int S_X_Jx = positionList[2] - positionList[1] -1; //读X所需的cost
        double benefit = 0.0;  // 节省下来的cost
        int Q_Join_X_Estimate = ((positionList[3])/(i-Ix0.get(datasetId)+1))*(Query.size()-Ix0.get(datasetId)+1);
        int Q_Join_Xk1_Est;
        if (Q_Join_X_Estimate <= qJoinXk){
            Q_Join_Xk1_Est = qJoinXk;
        }else if (Q_Join_X_Estimate > qJoinXk && Q_Join_X_Estimate <= qJoinXk_1){
            Q_Join_Xk1_Est = Q_Join_X_Estimate;
        }else{
            Q_Join_Xk1_Est = qJoinXk_1;
        }
        int prefix1 = Query.size() - Q_Join_Xk1_Est + 1;
        int L = 0;
        int S  = 0 ;
        if (prefix1+1 <= p){
            for (int t = prefix1+1; t <= p; t++){
                Long queryId = Query.get(t);
//                L += InvertedIndex.get(queryId).size(); //////////注意，这里是集合数还是？
                for (int[] array:InvertedIndex.get(queryId)){
                    L += array[2];
                }
            }
        //HashMap<Integer, int[]> U
        for (int id: U.keySet()){
            int indicator = 0;
            if (id != queryID){
                int[] YpositionList = U.get(id);
                int Q_Join_Y_upperbound =  YpositionList[3]+ Math.min(Query.size()-i, YpositionList[2]-YpositionList[1]);
                if (Q_Join_Y_upperbound <= Q_Join_Xk1_Est){
                    indicator = 1;
                    S += (YpositionList[2]- YpositionList[1] -1)*indicator;
                }
            }
        }
        }
        benefit = L+S;
        return  S_X_Jx-benefit;
    }

    public static void PositionFilter( ArrayList<Long> sortedQuery, int i,int qJoinXk, HashMap<Integer, int[]> U){
        ArrayList<Integer> removeList = new ArrayList<>();
        for (int j: U.keySet()) {
            int[] positionList = U.get(j);
            double upperBound = positionList[3] + Math.min(sortedQuery.size()-i, positionList[2]-positionList[1]);
            if (upperBound < qJoinXk){
                removeList.add(j);
            }
        }
        for (int t = 0; t<removeList.size(); t++){
            U.remove(removeList.get(t));
        }
    }

    public static void batchReadCandidate(int i, int j, ArrayList<Long> sortedQuery, HashMap<Long, ArrayList<int[]>> InvertedIndex, HashMap<Integer, Integer> Ix0, HashMap<Integer, int[]> U){
        //(candidateID, (first_token_position, recent_token_position, X_set_total_num, intersection_num))
        for (int t =i; t < j; t++){
            long token = sortedQuery.get(t);
//            System.out.print("token = "+token+"; ");
//            System.out.println( InvertedIndex.get(token));
            if (InvertedIndex.containsKey(token)){
                ArrayList<int[]> candidate = InvertedIndex.get(token);
                for (int[] posting: candidate){
                    if(!Ix0.containsKey(posting[0]))
                        Ix0.put(posting[0],t);
                    if (U.containsKey(posting[0])){
//                        System.out.print(posting[0]);
                        int[] positionList = U.get(posting[0]);
                        positionList[1] = posting[2];
                        positionList[3] += 1;
                        U.put(posting[0], positionList);
                    }else{
                        int[] positionList = new int[4];
                        positionList[0] = posting[1];
                        positionList[1] = posting[1];
                        positionList[2] = posting[2];
                        positionList[3] = 1;
                        U.put(posting[0], positionList);
                    }
                }
            }
//            System.out.print("!!U.isEmpty() = "+ U.isEmpty());


        }
    }

//    public static PriorityQueue<relaxIndexNode> Exactjosie(HashMap<Long, ArrayList<int[]>> InvertedIndex, HashMap<Integer, ArrayList<Long>> datasetIdMapSignature, int batchSize, ArrayList<Long> Query, int topK, int queryID){
//        System.out.println("InvertedIndex.size()"+InvertedIndex.size());
//        ArrayList<Long> sortedQuery = sortArray(Query);
//        int queryNumber = sortedQuery.size();
//        PriorityQueue<relaxIndexNode> heap = new PriorityQueue(new ComparatorByRelaxIndexNodeReverse()); //large -> small
//        HashMap<String, Integer> Ix0 = new HashMap<>();
//        HashMap<String, int[]> U = new HashMap<>(); //Unread candidates
//        int i = 1;
//        int i1 = batchSize+1;
//        if (i1 > Query.size()){
//            i1 = Query.size();
//        }
//        ArrayList<Long> Xk = null;
//        int qJoinXk_1 = 0;
//        int qJoinXk = 0;
//        int prefix = queryNumber-qJoinXk+1;
////        System.out.println("prefix = "+prefix);
//
//        while (i <= prefix || !U.isEmpty()){ //prefix是在不断更新的，通过增加阈值，减少prefix
//            // X <- Best(u)
//            ArrayList<Long> X = new ArrayList<>();
//            String Xid = "-1";
//            double NetCostX = 0;
//            double NetCostBi = 0;
//            //Best(U)
//            if (U.isEmpty()){  //判断 empty or null
//                X = new ArrayList<>();
//            }else{
//                HashMap<String, Double> netCostBHm = new HashMap<>();
//                PriorityQueue<relaxIndexNode> netCostPQ = new PriorityQueue<>(new ComparatorByRelaxIndexNode());
//                //循环重复
//                for (String j: U.keySet()){
////                    System.out.println("j = "+j);
//                    String[] positionList = U.get(j);
//                    // 计算每个候选集的net_cost值
//                    double net_cost = NetCost(qJoinXk_1, qJoinXk, InvertedIndex, i, j, positionList, sortedQuery, prefix, U, Ix0, queryID);
////                    System.out.println("net_cost = "+net_cost);
//                    relaxIndexNode re = new relaxIndexNode(j, net_cost);
//                    double net_costB =  NetCostB(datasetIdMapSignature.get(j), i, i1, qJoinXk, InvertedIndex, j, positionList, sortedQuery, prefix, U, Ix0, queryID);
////                    System.out.println("net_costB = "+net_costB);
//                    netCostBHm.put(j, net_costB);
//                    netCostPQ.add(re);
//                }
//                relaxIndexNode xRelaxNode = netCostPQ.poll();
//                X = datasetIdMapSignature.get(xRelaxNode.resultId);
//                Xid = xRelaxNode.resultId;
//                NetCostX = xRelaxNode.getLb();
//                NetCostBi = netCostBHm.get(xRelaxNode.resultId);
////                System.out.println("Xid = "+Xid +", NetCostX = "+NetCostX +", NetCostBi = "+NetCostBi);
//            }
////            System.out.println("X.isEmpty() = "+X.isEmpty());
////            System.out.println("U.isEmpty() = "+U.isEmpty());
//            if (heap.size() == topK && NetCostX > NetCostBi || (U.isEmpty() && X.isEmpty())){
//                batchReadCandidate(i-1, i1-1, sortedQuery, InvertedIndex, Ix0, U);
////                System.out.println("U.size() = "+U.size());
//                i = i1;
//                i1 = i1+batchSize;
//                if (i1 > Query.size()){
//                    i1 = Query.size();
//                }
//            }else
//            {
//                relaxIndexNode re = new relaxIndexNode(Xid, Silo.exactCompute(X, Query));
//                heap.add(re);
//                U.remove(Xid);
//                while (heap.size()>topK){
//                    heap.poll();
//                }
//                Xk = datasetIdMapSignature.get(heap.peek().resultId);
//                int t = (int)heap.peek().getLb();
////                System.out.println("t = "+t);
//                if (qJoinXk < t){
//                    qJoinXk_1 = qJoinXk;
//                    qJoinXk = t;
//                }else {
//                    if (qJoinXk_1 < t)
//                        qJoinXk_1 = t;
//                }
//                prefix = Query.size() - qJoinXk + 1;
//            }
//            PositionFilter(sortedQuery, i, qJoinXk, U);
////            System.out.println("PositionFilter U.size = "+U.size());
//        }
//        return heap;
//    }
    public static PriorityQueue<relaxIndexNode> josie(TreeMap<Long, TreeMap<String, int[]>> InvertedIndex, HashMap<String, ArrayList<Long>> datasetIdMapSignature, ArrayList<Long> Query, int topK){
        ArrayList<Long> sortedQuery = sortArray(Query);
        int queryNumber = sortedQuery.size();
//        System.out.println("queryNumber = "+queryNumber);
        PriorityQueue<relaxIndexNode> heap = new PriorityQueue(new ComparatorByRelaxIndexNode()); //  large->small

        HashMap<String, Integer> candiHm = new HashMap<>();
        HashSet<String> ExactAndFilter = new HashSet<>();
        HashMap<String,Double> exactResultHm = new HashMap<>();
        TreeMap<String, Double> resultHm = new TreeMap<String, Double>();
        TreeMap<Double, String> thresholdTM = new TreeMap<Double, String>(
                                new Comparator<Double>(){
            public int compare(Double a,Double b){  // large -> small
                return (int)(b-a);
            }
        }
        );

        double threshold = 1;
        int prefix = queryNumber-(int) Math.round(threshold) + 1;
//        System.out.println("prefix = "+prefix+ ", topK = "+topK);
        for (int i = 0; i < prefix; i++){
            long l = sortedQuery.get(i);
//            System.out.println("l =" +l);
            if (InvertedIndex.containsKey(l)){
                TreeMap<String, int[]> invertIndex = InvertedIndex.get(l);
                for (String a: invertIndex.keySet()){
                    int[] ar = invertIndex.get(a);//
                    if (exactResultHm.size() < topK && !exactResultHm.containsKey(a)){
                        ArrayList<Long> dataList = datasetIdMapSignature.get(a);
                        int intersection = Silo.exactCompute(sortedQuery, dataList);
                        resultHm.put(a, intersection*1.0);
                        exactResultHm.put(a, intersection*1.0);
//                        heap.add(new relaxIndexNode(ar[0], intersection*1.0));
//                        System.out.println("r："+ar[0]+", "+intersection*1.0);
                        ExactAndFilter.add(a);
                        thresholdTM.put(intersection*1.0, a);

                    }else if (exactResultHm.containsKey(a)){
//                        System.out.println("have been computed!");
                    } else {
//                        System.out.println("ar[0] = "+ar[0]);
                        if (ExactAndFilter.contains(a)){
//                            System.out.println("filter ");
                        }//跳过
                        else{
                            int count = 0;
                            for(double id: thresholdTM.keySet()){
                                count++;
                                if (count <= topK){
                                    threshold = id;
                                }
                            }
//                            System.out.println("!!threshold = " + threshold);

                            //先计算UB，如果UB< threshold 则过滤
                            double t = resultHm.getOrDefault(a, 0.0);
                            double lastInterPosition = candiHm.getOrDefault(a, 0);
                            double UB = (t+1.0) + Math.min(queryNumber-i+1, ar[1] - lastInterPosition);
                            candiHm.put(a, ar[0]);
                            resultHm.put(a, (t+1));
                            thresholdTM.put((t+1.0), a);
//                            System.out.println("UB = "+UB+", (t+1) = "+(t+1)+", ar[0] = "+ar[0]);
                            if (UB < threshold){
                                ExactAndFilter.add(a);
                            }

                        }
                    }

                }
            }
            prefix = queryNumber-(int) Math.round(threshold) + 1;
        }

        for (String i: resultHm.keySet()){
            relaxIndexNode re = new relaxIndexNode(i, resultHm.get(i));
            heap.add(re);
        }

        PriorityQueue<relaxIndexNode> finalResultHeap = new PriorityQueue(new ComparatorByRelaxIndexNode()); // large-> small
        int t = Math.min(topK, heap.size());
//        System.out.println("t = "+t);
        for (int count2 = 0; count2<t; count2++){
//            System.out.println("topk = "+topK);
            relaxIndexNode re = heap.poll();
            finalResultHeap.add(re);
        }

        return finalResultHeap;
    }

    public static void main(String[] args){
        HashMap<String, ArrayList<Long>> dataset = new HashMap<>();
        ArrayList<Long> t1 = new ArrayList<>();
        t1.add((long)1);
        t1.add((long)200);
        t1.add((long)100);
        dataset.put("1", t1);
        ArrayList<Long> t2 = new ArrayList<>();
        t2.add((long)5);
        t2.add((long)2);
        dataset.put("2", t2);
        ArrayList<Long> t3 = new ArrayList<>();
        t3.add((long)2);
        dataset.put("3", t3);
        ArrayList<Long> t4 = new ArrayList<>();
        t4.add((long)101);
        t4.add((long)2);
        t4.add((long)1);
        dataset.put("4", t4);

        TreeMap<Long, TreeMap<String, int[]>> postlisitngHashMap = createJosiePostinglists(dataset);
        for (long i: postlisitngHashMap.keySet()){
            System.out.print(i+" : ");
            TreeMap<String, int[]> array = postlisitngHashMap.get(i);
            for (String j : array.keySet()){
                int[] posting = array.get(j);
                System.out.print(j+", "+posting[0]+", "+posting[1]+"; ");
            }
            System.out.println();
        }


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

/*
    public static void josie_ori(HashMap<Long, ArrayList<int[]>> InvertedIndex, HashMap<Integer, ArrayList<Long>> datasetIdMapSignature, int batchSize, ArrayList<Long> Query, int topK, int queryID){
            ArrayList<Long> sortedQuery = sortArray(Query);
            int queryNumber = sortedQuery.size();
            PriorityQueue heap = new PriorityQueue();
            HashMap<Integer, int[]> U = new HashMap<Integer, int[]>();
            HashMap<Integer, Integer> Ix0 = new HashMap<>();
            //(candidateID, (first_token_position, recent_token_position, X_set_total_num, intersection_num))
            for (int t =0; t<batchSize; t++){
                        long token = sortedQuery.get(t);
                        ArrayList<int[]> candidate = InvertedIndex.get(token);
                        for (int[] posting: candidate){
                            if(!Ix0.containsKey(posting[0]))
                                Ix0.put(posting[0],t);
                            if (U.containsKey(posting[0])){
                                int[] positionList = U.get(posting[0]);
                                positionList[1] = posting[2];
                                positionList[3] += 1;
                                U.put(posting[0], positionList);
                            }else{
                        int[] positionList = new int[4];
                        positionList[0] = posting[1];
                        positionList[1] = posting[1];
                        positionList[2] = posting[2];
                        positionList[3] = 1;
                        U.put(posting[0], positionList);
                    }
                }
            }
            int i =1;
            int i1 = batchSize+1;
            //需要增加判断if (U.size > topK) 找阈值，否则再读一批，直到U.size > topK
            //是否应该读K个候选集，确定阈值？
//            int count = 0;
//            int qJoinXk = 0;
//            ArrayList<Long> Xk;
//            for (int t: U.keySet()){
//                count++;
//                Xk = datasetIdMapSignature.get(t);
//                qJoinXk = Silo.exactCompute(sortedQuery, Xk);
//                U.remove(t);
//                if (count >= topK)
//                    break;
//            }

            ArrayList<Long> Xk = null;
            int qJoinXk = Silo.exactCompute(sortedQuery, Xk);
            int prefix = queryNumber-qJoinXk+1;
            while (i <= prefix || !U.isEmpty()){ //prefix是在不断更新的，通过增加阈值，减少prefix
                // X <- Best(u)
                HashMap<Integer, Double> netCostHm = new HashMap<>();
                PriorityQueue<relaxIndexNode> netCostPQ = new PriorityQueue<>(new ComparatorByRelaxIndexNode());
                for (int j: U.keySet()){
                    int[] positionList = U.get(j);
                    double net_cost = 0.0;
                    // 计算每个候选集的net_cost值
                    net_cost = NetCost(qJoinXk, InvertedIndex, i, j, positionList, datasetIdMapSignature, sortedQuery, prefix, U, Ix0, queryID);
                    relaxIndexNode re = new relaxIndexNode(j, net_cost);
                    netCostHm.put(j, net_cost);
                    netCostPQ.add(re);
                }
                relaxIndexNode xRelaxNode = netCostPQ.poll();
                double threshold = Double.MIN_VALUE;
                ArrayList<Long> X = datasetIdMapSignature.get(xRelaxNode.resultId);


                if (heap.size() == topK && threshold > NetCostB(X, i, i1, ) || (U.isEmpty() && X.isEmpty())){
                    U = U ;// + PostingList
                    i = i1;
                    i1 = i1+batchSize;
                }else{

                }


            }




    }
*/