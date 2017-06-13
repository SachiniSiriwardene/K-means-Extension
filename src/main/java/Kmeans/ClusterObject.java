package Kmeans;

public class ClusterObject {

    /**
     * Object which holds the data to be clustered and the
     * index of the centroid to which it belongs to
     */

    private double value;
    private int index;


    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }


}
