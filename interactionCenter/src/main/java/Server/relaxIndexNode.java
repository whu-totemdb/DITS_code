package Server;

public class relaxIndexNode {
    public String resultId;
    public double lb;
    public double ub;
    public relaxIndexNode(String id){
        this.resultId = id;
    }
    public relaxIndexNode(String id, double lb){
        this.resultId = id;
        this.lb = lb;
    }
    public relaxIndexNode(String id, double lb, double ub){
        this.resultId = id;
        this.lb = lb;
        this.ub = ub;
    }
    public String getResultId(){
        return this.resultId;
    }

    public void setLb(double lb){
        this.lb = lb;
    }
    public double getLb(){
        return this.lb;
    }

    public void setUb(double ub){
        this.ub = ub;
    }
    public double getUb(){
        return this.ub;
    }
}
