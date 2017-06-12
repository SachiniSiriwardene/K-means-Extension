package Kmeans;

import java.util.ArrayList;


public class Clusterer {
    private static int noOfClusters;
    private static int maxIter;
    private static ArrayList<Double>[] clusterGroup;
    private static ArrayList<Double> center = new ArrayList<>();
    private static ArrayList<Double> centerOld = new ArrayList<>();
    private static ArrayList<Double> distinctValues = new ArrayList<>();
    private static int distinctCount = 0;

    public static void setNoOfClusters(int noOfClusters) {
        Clusterer.noOfClusters = noOfClusters;
    }

    public static void setMaxIter(int maxIter) {
        Clusterer.maxIter = maxIter;
    }

    public static void setClusterGroup(ArrayList<Double>[] clusterGroup) {
        Clusterer.clusterGroup = clusterGroup;
    }

    /**
     * Perform clustering
     */
    public static void cluster(ArrayList<Double> data) {
        clearData();
        initialize(data);
        int iter = 0;
        if (data.size() != 0) {
            do {
                if (!center.equals(centerOld)) {
                    centerOld = new ArrayList<Double>();
                    for (int i = 0; i < clusterGroup.length; i++) {
                        clusterGroup[i] = new ArrayList<>();

                    }
                }
                assignToCluster(data);
                reinitializeCluster();
                iter++;
            } while (!center.equals(centerOld) && iter < maxIter);
        }
    }

    /**
     * initializing cluster centers
     */
    public static void initialize(ArrayList<Double> data) {
        distinctCount = 0;
        distinctValues.clear();
        center.clear();
        for (int i = 0; i < data.size(); i++) {
            if (distinctCount >= noOfClusters) {
                break;
            }
            double value = data.get(i);
            if (!distinctValues.contains(value)) {
                distinctValues.add(value);
                center.add(value);
                clusterGroup[distinctCount] = new ArrayList<>();
                distinctCount++;

            }
        }
        if (distinctCount < noOfClusters) {
            noOfClusters = distinctCount;
        }
    }


    /**
     * reinitialize the cluster centres and store the old ones
     */
    private static void reinitializeCluster() {
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
    private static Double average(ArrayList<Double> doubles) {
        double sum = 0;
        for (int i = 0; i < doubles.size(); i++) {
            sum = sum + doubles.get(i);

        }

        return (sum / doubles.size());
    }

    /**
     * calculates the nearest center to each data point and adds the data to the cluster of respective center
     */
    private static void assignToCluster(ArrayList<Double> data) {
       Object[] output ;
       double value;
        for (int i = 0; i < data.size(); i++) {
            value = data.get(i);
           output = getCenter(value);
           int index = (Integer)output[1];
           clusterGroup[index].add(value);
        }
    }


    /**
     *
     * @param value - the value for which the center it belongs to should be returned
     * @return
     */

    public static Object[] getCenter(double value) {

        double centerValue, difference, currentDifference;
        int index;

        difference = Math.abs(center.get(0) - value);
        difference = Math.round(difference * 10000.0) / 10000.0;
        centerValue = center.get(0);
        index = 0;
        for (int j = 1; j < noOfClusters; j++) {
            currentDifference =  Math.abs(center.get(j) - value);
            currentDifference = Math.round(currentDifference * 10000.0) / 10000.0;
            if(difference > currentDifference){
                difference = currentDifference;
                centerValue = center.get(j);
                index = j;
            }else if(currentDifference == difference){
                if(centerValue < center.get(j)){
                    centerValue = center.get(j);
                    index = j;
                }
            }
        }

        Object[] center = {centerValue, index, difference};
        return center;
    }

    /**
     * assign new arralists to hold the data for each cluster center
     * during training
     */
    public static void clearData() {
        for (int i = 0; i < clusterGroup.length; i++) {
            clusterGroup[i] = new ArrayList<Double>();

        }
    }
}
