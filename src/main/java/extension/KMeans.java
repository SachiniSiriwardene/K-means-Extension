package extension;

import Kmeans.ClusterObject;
import Kmeans.Clusterer;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Extension(
        name = "cluster",
        namespace = "kmeans",
        description = "Performs single dimension clustering on a given data set for a given window implementation",
        parameters = {
                @Parameter(name = "value",
                        description = "Value to be clustered",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "clustercenters",
                        description = "Number of cluster centers",
                        type = {DataType.INT}),
                @Parameter(name = "iterations",
                        description = "Number of iterations",
                        type = DataType.INT),
                @Parameter(name = "eventstotrain",
                        description = "New cluster centers are found for given number of events",
                        type = DataType.INT),
                @Parameter(name = "continuetraining",
                        optional = true,
                        description = "train the model for available amount of data",
                        type = DataType.BOOL)

        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "matchedClusterCentroid",
                        description = "Returns cluster center to which data point belongs to",
                        type = {DataType.DOUBLE}),
                @ReturnAttribute(
                        name = "matchedClusterIndex",
                        description = "the index of the cluster center",
                        type = {DataType.INT}
                ),
                @ReturnAttribute(
                        name = "distanceToCenter",
                        description = "the difference between the value and the cluster center",
                        type = {DataType.DOUBLE}
                )
        },
        examples = @Example(syntax = "\"from InputStream#window.length(5)#kmeans:cluster(value, 4, 20, 5) \"\n" +
                " select value, matchedClusterCentroid, matchedClusterIndex, distanceToCenter \"\n" +
                " insert into OutputStream;",
                description = "This will cluster the collected values within the window for every 5 events" +
                        "and give the output after the first 5 events.")
)
public class KMeans extends StreamProcessor {
    private int clusters;
    private int iterations;
    private boolean trainNow;
    private int eventsToTrain;
    private int count;
    private ArrayList<ClusterObject> clusterDataList = new ArrayList<>();
    private double value;
    private boolean trainModel;
    private boolean modelTrained;
    private Clusterer clusterer;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
                count++;
                value = (Double) attributeExpressionExecutors[0].execute(streamEvent);
                ClusterObject clusterData = new ClusterObject();
                clusterData.setValue(value);
                clusterDataList.add(clusterData);
                Object[] outputData = null;
                if (count > eventsToTrain && eventsToTrain != 0) {
                    outputData = clusterer.getCenter(value);
                }
                if (modelTrained) {
                    outputData = clusterer.getCenter(value);
                }
                if (trainModel) {
                    trainNow = (Boolean) attributeExpressionExecutors[3].execute(streamEvent);
                    if (trainNow) {
                        clusterer.cluster(clusterDataList);
                        modelTrained = true;
                    }
                }
                if (eventsToTrain > 0) {
                    if (count % eventsToTrain == 0) {
                        clusterer.cluster(clusterDataList);
                    }
                }
                if (outputData == null) {
                    streamEventChunk.remove();
                } else {
                    complexEventPopulater.populateComplexEvent(streamEvent, outputData);
                }
            } else if (streamEvent.getType() == ComplexEvent.Type.RESET) {
                clusterDataList.clear();
                continue;
            } else if (streamEvent.getType() == ComplexEvent.Type.EXPIRED) {
                clusterDataList.remove(0);
                continue;
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                   ConfigReader configReader, ExecutionPlanContext executionPlanContext) {

        if (!(attributeExpressionExecutors.length == 4)) {
            throw new ExecutionPlanValidationException(
                    "Clustering function has to have exactly 4 parameters, currently "
                            + attributeExpressionExecutors.length + " parameters provided.");

        }

        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new ExecutionPlanValidationException("Cluster centers has to be a constant.");
        }
        if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
            throw new ExecutionPlanValidationException("Iterations centers has to be a constant.");
        }

        Object clustersObject = attributeExpressionExecutors[1].execute(null);
        if (clustersObject instanceof Integer) {
            clusters = (Integer) clustersObject;
        } else {
            throw new ExecutionPlanValidationException("Cluster centers should be of type int. But found "
                    + attributeExpressionExecutors[2].getReturnType());
        }

        Object iterationsObject = attributeExpressionExecutors[2].execute(null);
        if (iterationsObject instanceof Integer) {
            iterations = (Integer) iterationsObject;

        } else {
            throw new ExecutionPlanValidationException("Iterations should be of type int. But found "
                    + attributeExpressionExecutors[2].getReturnType());
        }

        if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.BOOL) {
            trainModel = true;

        } else if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
            Object trainingOptionObject = attributeExpressionExecutors[3].execute(null);
            if (trainingOptionObject instanceof Integer) {
                eventsToTrain = (Integer) trainingOptionObject;

            } else {
                throw new ExecutionPlanValidationException("No. of events to train should be of type integer. " +
                        "But found " + attributeExpressionExecutors[3].getReturnType());
            }
        } else {
            throw new ExecutionPlanValidationException("No. of events to train should be of type boolean. But found "
                    + attributeExpressionExecutors[3].getReturnType());
        }

        clusterer = new Clusterer(clusters, iterations);


        List<Attribute> attributeList = new ArrayList<Attribute>(3);
        attributeList.add(new Attribute("matchedClusterCentroid", Attribute.Type.DOUBLE));
        attributeList.add(new Attribute("matchedClusterIndex", Attribute.Type.INT));
        attributeList.add(new Attribute("distanceToCenter", Attribute.Type.DOUBLE));
        return attributeList;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> map = new HashMap();
        map.put("data", clusterDataList);
        map.put("trainModel", trainModel);
        map.put("modelTrained", modelTrained);
        map.put("trainingState", trainNow);
        map.put("count", count);
        map.put("eventsToTrain", eventsToTrain);
        map.put("value", value);
        return map;
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        clusterDataList = (ArrayList<ClusterObject>) map.get("data");
        trainModel = (Boolean) map.get("trainModel");
        modelTrained = (Boolean) map.get("modelTrained");
        trainNow = (Boolean) map.get("trainingState");
        value = (Double) map.get("value");
        count = (Integer) map.get("count");
        eventsToTrain = (Integer) map.get("eventsToTrain");


    }


}
