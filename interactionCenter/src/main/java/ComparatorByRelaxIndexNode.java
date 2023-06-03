

import Server.relaxIndexNode;

import java.util.Comparator;
import java.util.PriorityQueue;

public class ComparatorByRelaxIndexNode implements Comparator { // large -> small
    public int compare(Object o1, Object o2){
        relaxIndexNode in1 = (relaxIndexNode)o1;
        relaxIndexNode in2 = (relaxIndexNode)o2;
        return (in2.getLb()-in1.getLb()>0)?1:-1;
    }
    public static void main(String[] args ){
        PriorityQueue<relaxIndexNode> finalResultHeap = new PriorityQueue(new ComparatorByRelaxIndexNode()); // small -> large
        finalResultHeap.add(new relaxIndexNode("3", 5.0));
        finalResultHeap.add(new relaxIndexNode("8", 9.0));
        finalResultHeap.add(new relaxIndexNode("4", 1.0));
        System.out.println(finalResultHeap.peek().getLb());

        for (relaxIndexNode re: finalResultHeap){
            System.out.println(re.resultId+"," +re.getLb());
        }
    }

}
