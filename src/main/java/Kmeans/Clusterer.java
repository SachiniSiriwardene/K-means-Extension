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

    public static int getNoOfClusters() {
        return noOfClusters;
    }

    public static void setNoOfClusters(int noOfClusters) {
        Clusterer.noOfClusters = noOfClusters;
    }

    public static int getMaxIter() {
        return maxIter;
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
        double[] difference;
        double dataItem, cenVal, diff;
        for (int i = 0; i < data.size(); i++) {
            difference = new double[noOfClusters];
            dataItem = data.get(i);
            for (int j = 0; j < noOfClusters; j++) {
                cenVal = center.get(j);
                diff = Math.abs(cenVal - dataItem);
                diff = Math.round(diff * 10000.0) / 10000.0;
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

    private static ArrayList<Integer> getMinIndexA(double[] diff) {
        int minIndex = 0;
        ArrayList<Integer> minList = new ArrayList<>();
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

    public static Object[] getCenter(double value) {
        double[] difference;
        double dataItem, cenVal, diff;
        int index;
        difference = new double[noOfClusters];
        dataItem = value;
        for (int j = 0; j < noOfClusters; j++) {
            cenVal = center.get(j);
            diff = Math.abs(cenVal - dataItem);
            diff = Math.round(diff * 10000.0) / 10000.0;
            difference[j] = diff;
        }
        ArrayList<Integer> minList = getMinIndexA(difference);
        if (minList.size() == 1) {
            index = minList.get(0);
            cenVal = center.get(index);
            diff = Math.abs(cenVal - dataItem);
            diff = Math.round(diff * 10000.0) / 10000.0;
        } else {
            int minIndex = minList.get(0);
            for (int j = 1; j < minList.size(); j++) {
                if (center.get(minIndex) < center.get(minList.get(j))) {
                    minIndex = minList.get(j);
                }
            }
            index = minIndex;
            cenVal = center.get(index);
            diff = Math.abs(cenVal - dataItem);
            diff = Math.round(diff * 10000.0) / 10000.0;

        }
        Object[] center = {cenVal, index, diff};
        return center;
    }


    public static void clearData() {
        for (int i = 0; i < clusterGroup.length; i++) {
            clusterGroup[i] = new ArrayList<Double>();

        }
    }
}
