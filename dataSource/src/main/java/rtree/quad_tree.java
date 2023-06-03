package rtree;

import Client.Silo;
import Utils.indexNode;
import com.github.varunpant.quadtree.Node;
import com.github.varunpant.quadtree.QuadTree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class quad_tree {

    public static int dimension = 2;
    public static HashMap<histogramKey, String> data_hm = new HashMap<>();
    public static QuadTree<String> qt;
    public static List<Node> allNodeofTree = new ArrayList<>();
    public static List<Node> InterSectNode = new ArrayList<>();

    public static QuadTree<String> buildTree(HashMap<String, ArrayList<Long>> zcodemap) throws CloneNotSupportedException {

        double minX, maxX, minY, maxY;
        minX = Double.MAX_VALUE;
        minY = Double.MAX_VALUE;
        maxX = -1000000000;
        maxY = -1000000000;
        int count = 0;
        for (String s : zcodemap.keySet()) {
            ArrayList<Long> zcodeHm = zcodemap.get(s);
            for (int i = 0; i<zcodeHm.size(); i++) {
                long l = zcodeHm.get(i);
                count++;
                double[] d = Silo.resolve(l, Silo.resolution);
                histogramKey f = new histogramKey(d[0], d[1]);
                data_hm.put(f, s);
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
        }
        qt = new QuadTree<>(minX, minY, maxX, maxY);
        for (histogramKey hk : data_hm.keySet()) {
            double x = hk.x;
            double y = hk.y;
            String w = data_hm.get(hk);
            //Note!!!!!!!!!!!!!!!!!
            qt.set(x, y, w);
        }

        return qt;
    }


    public static void traverse2(Node node) {
        switch (node.getNodeType()) {
            case LEAF:
                allNodeofTree.add(node);
                break;
            case POINTER:
                allNodeofTree.add(node);
                traverse2(node.getNe());
                traverse2(node.getSe());
                traverse2(node.getSw());
                traverse2(node.getNw());
                break;
        }
    }

    public static boolean intersected(Node node, indexNode query, int dim) {
        double[] mbrmin = {node.getX(), node.getY()};
        double[] mbrmax = {node.getX() + node.getW(), node.getY() + node.getH()};
        double[] querymin = query.getMBRmin();
        double[] querymax = query.getMBRmax();
        for(int i=0; i<dim; i++)
            if(mbrmin[i] > querymax[i] || mbrmax[i] < querymin[i]) {
                return false;
            }
        return true;
    }


    public static void traverse(Node node, indexNode queryNode) {
        switch (node.getNodeType()) {
            case LEAF:
                if (intersected(node, queryNode, dimension)){
                    InterSectNode.add(node);
                }
                break;
            case POINTER:
               //如果相交
                if (intersected(node.getNe(), queryNode, dimension)){
                    traverse(node.getNe(),  queryNode);
                }if (intersected(node.getSe(), queryNode, dimension)){
                    traverse(node.getSe(),  queryNode);
                }if (intersected(node.getSw(), queryNode, dimension)){
                    traverse(node.getSw(),  queryNode);
                }if (intersected(node.getNw(), queryNode, dimension)){
                    traverse(node.getNw(),  queryNode);
                }
                break;
        }
    }

    public static List<Node> getAllInterSectNode(indexNode queryNode){
        traverse(qt.getRootNode(), queryNode);

        return InterSectNode;
    }


    public static int getMaxLevel() {
        traverse2(qt.getRootNode());
        int maxLevel = 0;
        for (int i = 0; i < allNodeofTree.size(); i++) {
            if (maxLevel < allNodeofTree.get(i).getLevel()) {
                maxLevel = allNodeofTree.get(i).getLevel();
            }
        }
        return maxLevel;
    }

    public static void traverse3(Node node) {
        switch (node.getNodeType()) {
            case LEAF:
                System.out.println("Leaf = "+node.getX()+", "+node.getY()+", "+node.getW()+", "+node.getH()+"; ");
//                List<Point> list= node.getPointList();
//                for (Point p: list){
//                    System.out.println("leaf = = "+ p.getX()+", "+p.getY()+", "+p.getValue());
//                }

                break;
            case POINTER:
                System.out.println("POINTER = "+node.getNe().getX()+", "+node.getNe().getY()+", "+node.getNe().getW()+", "+node.getNe().getH()+"; ");
                System.out.println("POINTER = "+node.getSe().getX()+", "+node.getSe().getY()+", "+node.getSe().getW()+", "+node.getSe().getH()+"; ");
                System.out.println("POINTER = "+node.getSw().getX()+", "+node.getSw().getY()+", "+node.getSw().getW()+", "+node.getSw().getH()+"; ");
                System.out.println("POINTER = "+node.getNw().getX()+", "+node.getNw().getY()+", "+node.getNw().getW()+", "+node.getNw().getH()+"; ");
                traverse3(node.getNe());
                traverse3(node.getSe());
                traverse3(node.getSw());
                traverse3(node.getNw());
                break;
        }
    }

    public static void main(String[] args){
        qt = new QuadTree<>(0, 0, 8, 8);
        qt.set(0,1, "0");
        qt.set(1,1, "1");
        qt.set(2,2, "2");
        qt.set(3,5, "3");
        qt.set(6,7, "4");
        qt.set(5,6, "5");
        qt.set(5,6, "6");
        qt.set(5,6, "7");
        qt.set(5,6, "8");
        qt.set(5,6, "9");
        qt.set(5,6, "10");
        qt.set(5,6, "11");
        System.out.println("qt.getCount() = "+qt.getCount());
        traverse3(qt.getRootNode());
    }

    static class histogramKey {
        double x;
        double y;

        public histogramKey(double x, double y) {
            this.x = x;
            this.y = y;
        }

//        public int hashCode() {
//            final int prime = 31;
//            int result = 1;
//            result = prime * result + (int) x;
//            result = prime * result + (int) y;
//            return result;
//        }

//        @Override
//        public boolean equals(Object obj) {
//            if (this == obj)
//                return true;
//            if (obj == null)
//                return false;
//            if (getClass() != obj.getClass())
//                return false;
//            histogramKey other = (histogramKey) obj;
//            if (x != other.x)
//                return false;
//            if (y != other.y)
//                return false;
//            return true;
//        }

    }
}

