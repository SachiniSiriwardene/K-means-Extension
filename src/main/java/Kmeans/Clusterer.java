package Kmeans;

import java.util.ArrayList;

/**
 * Created by sachini on 6/2/17.
 */
public class Clusterer {
    private int noOfClusters;
    private int maxIter;
    private ArrayList<Double> data;
    private int dataSize;
    private ArrayList<Double>[] clusterGroup;
    private ArrayList<Double> center;
    private ArrayList<Double> centerOld;

    /**
     * @param noOfClusters the number of cluster centers
     * @param maxIter      maximum number of iterations
     * @param dataSet      dataset
     */

    public Clusterer(int noOfClusters, int maxIter, ArrayList<Double> dataSet) {
        this.noOfClusters = noOfClusters;
        this.maxIter = maxIter;
        this.data = dataSet;
        this.center = new ArrayList<Double>();
        this.centerOld = new ArrayList<Double>();
        //check if the no. of data points are less than the no. of clusters
        this.dataSize = data.size();
        if (this.noOfClusters > dataSize) {
            this.noOfClusters = dataSize;
        }
        this.clusterGroup = new ArrayList[this.noOfClusters];
        //get the first k distinct values from the data. This will be used to initialize the cluster centers
        int distinctCount = 0;
        ArrayList<Double> distinctValues = new ArrayList<Double>();
        for (int i = 0; i < dataSize; i++) {
            if (!distinctValues.contains(data.get(i))) {
                distinctValues.add(data.get(i));
                center.add(data.get(i));
                this.clusterGroup[distinctCount] = new ArrayList<Double>();
                distinctCount++;
            }
            if (center.size() == this.noOfClusters) {
                break;
            }
        }
        // if k values are not found, k should be the same as the no of distinct values found
        this.noOfClusters = center.size();
    }

    /**
     * Perform clustering
     */
    public void cluster() {
        int iter = 0;
        do {
            if (!center.equals(centerOld)) {
                centerOld = new ArrayList<Double>();
                for (int i = 0; i < clusterGroup.length; i++) {
                    clusterGroup[i] = new ArrayList<Double>();

                }
            }
            assignToCluster();
            reinitializeCluster();
            iter++;
        } while (!center.equals(centerOld) && iter < maxIter);
    }

    /**
     * reinitialize the cluster centres and store the old ones
     */
    private void reinitializeCluster() {
        for (int i = 0; i < noOfClusters; i++) {
            centerOld.add(i, center.get(i));
            if (!clusterGroup[i].isEmpty()) {
                center.set(i, average(clusterGroup[i]));
            }
        }
    }

    /**
     * base on the data points assigned to the cluster, recalculates the cluster center
     *
     * @param doubles the cluster
     * @return the new cluster center
     */
    private Double average(ArrayList<Double> doubles) {
        double sum = 0;
        for (int i = 0; i < doubles.size(); i++) {
            sum = sum + doubles.get(i);
        }
        return (sum / doubles.size());
    }

    /**
     * calculates the nearest center to each data point and adds the data to the cluster of respective center
     */
    private void assignToCluster() {
        double[] difference;
        double dataItem, cenVal, diff;
        for (int i = 0; i < dataSize; i++) {
            difference = new double[noOfClusters];
            dataItem = data.get(i);
            for (int j = 0; j < noOfClusters; j++) {
                cenVal = center.get(j);
                diff = Math.abs(cenVal - dataItem);
                //diff = Math.round(diff * 10000.0) / 10000.0;
                difference[j] = diff;
            }
            ArrayList<Integer> minList = getMinIndexA(difference);
            if (minList.size() == 1) {
                clusterGroup[minList.get(0)].add(dataItem);
            } else {

                int minIndex = minList.get(0);
                for (int j = 1; j < minList.size(); j++) {
                    if (center.get(minIndex) < center.get(minList.get(j))) {
                        minIndex = minList.get(j);
                    }
                }
                clusterGroup[minIndex].add(dataItem);
            }
        }
    }

    private ArrayList<Integer> getMinIndexA(double[] diff) {
        int minIndex = 0;
        ArrayList<Integer> minList = new ArrayList<Integer>();
        for (int i = 1; i < diff.length; i++) {
            if (diff[minIndex] > diff[i]) {
                minIndex = i;
            }
        }
        minList.add(minIndex);
        for (int i = 0; i < diff.length; i++) {
            if (diff[minIndex] == diff[i] && minIndex != i) {
                //if (Math.abs(diff[minIndex] - diff[i]) < 0.0001 && minIndex != i) {
                minList.add(i);
            }
        }
        return minList;
    }

    public ArrayList<Integer> getCenterA(ArrayList<Double> data) {
        ArrayList<Integer> output = new ArrayList<Integer>();
        for (int i = 0; i < data.size(); i++) {
            for (int j = 0; j < noOfClusters; j++) {
                if (clusterGroup[j].contains(data.get(i))) {
                    output.add(j + 1);
                }
            }
        }
        return output;
    }
}
