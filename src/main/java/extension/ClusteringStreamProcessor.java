package extension;

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
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Extension(
        name = "cluster",
        namespace = "kmeans",
        description = "Performs single dimension clustering on a given data set for a given indow implementation",
        parameters = {
                @Parameter(name = "data",
                        description = "value to be clustered, no. of cluster centers, no. of iterations," +
                                       "no. of events each for which the model is trained, continue training",
                        type = {DataType.DOUBLE, DataType.INT, DataType.INT, DataType.INT, DataType.BOOL}),

        },
       /* returnAttributes = @ReturnAttribute(
                description = "Returns cluster center to which data point belongs to, the index of the cluster center,"
                               + "the difference between the value and the cluster center",
                type = {DataType.DOUBLE, DataType.INT, DataType.DOUBLE}),*/
        examples = @Example(description = "TBD", syntax = "TBD")
)
public class ClusteringStreamProcessor extends StreamProcessor implements SchedulingProcessor {
    private static Clusterer clusterer = new Clusterer();
    private int clusters;
    private int iterations;
    private boolean continueTraining;
    private int eventsToTrain;
    private int count;
    private ArrayList<Double> data = new ArrayList<>();
    private Scheduler scheduler;
    private double value;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
                count++;
                value = (Double) attributeExpressionExecutors[0].execute(streamEvent);
                data.add(value);
                Object[] outputData = null;
                if (count > eventsToTrain) {
                    outputData = clusterer.getCenter(value);
                }
                if (continueTraining) {
                    if (count % eventsToTrain == 0) {
                        clusterer.cluster(data);
                    }
                }else {
                    if (count == eventsToTrain) {
                        clusterer.cluster(data);
                        clusterer.clearData();
                    }
                }
                if (outputData == null) {
                    streamEventChunk.remove();
                } else {
                    complexEventPopulater.populateComplexEvent(streamEvent, outputData);
                }
            } else if (streamEvent.getType() == ComplexEvent.Type.RESET) {
                if(count > eventsToTrain) {
                    data.clear();
                    continue;
                }
            } else if (streamEvent.getType() == ComplexEvent.Type.EXPIRED) {
                if(count > eventsToTrain) {
                    data.remove(0);
                    continue;
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors,
                                   ConfigReader configReader, ExecutionPlanContext executionPlanContext) {

        if (!(attributeExpressionExecutors.length == 5)) {
            throw new ExecutionPlanValidationException(
                    "Clustering function has to have exactly 5 parameters, currently "
                            + attributeExpressionExecutors.length + " parameters provided.");

        }

        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new ExecutionPlanValidationException("Cluster centers has to be a constant.");
        }
        if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
            throw new ExecutionPlanValidationException("Iterations centers has to be a constant.");
        }
        if (!(attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor)) {
            throw new ExecutionPlanValidationException("No. of events to train model has to be a constant.");
        }



        Object clustersObject = attributeExpressionExecutors[1].execute(null);
        if (clustersObject instanceof Integer) {
            clusters = (Integer) clustersObject;
            clusterer.setNoOfClusters(clusters);
            ArrayList<Double>[] clusterGroup = new ArrayList[clusters];
            clusterer.setClusterGroup(clusterGroup);
        }  else {
            throw new ExecutionPlanValidationException("Cluster centers should be of type int. But found "
                    + attributeExpressionExecutors[2].getReturnType());
        }


        Object iterationsObject = attributeExpressionExecutors[2].execute(null);
        if (iterationsObject instanceof Integer) {
            iterations = (Integer) iterationsObject;
            clusterer.setMaxIter(iterations);
        }  else {
            throw new ExecutionPlanValidationException("Iterations should be of type int. But found "
                    + attributeExpressionExecutors[2].getReturnType());
        }



        Object eventsToTrainObject = attributeExpressionExecutors[3].execute(null);
        if (eventsToTrainObject instanceof Integer) {
            eventsToTrain = (Integer) eventsToTrainObject;
        }  else {
            throw new ExecutionPlanValidationException("Events to train should be of type int. But found "
                    + attributeExpressionExecutors[2].getReturnType());
        }


        Object continueTrainObject = attributeExpressionExecutors[4].execute(null);
        if (continueTrainObject instanceof Boolean) {
            continueTraining = (Boolean) continueTrainObject;
        }  else {
            throw new ExecutionPlanValidationException("Continue to train should be of type boolean. But found "
                    + attributeExpressionExecutors[2].getReturnType());
        }


        List<Attribute> attributeList = new ArrayList<Attribute>(3);
        attributeList.add(new Attribute("center", Attribute.Type.DOUBLE));
        attributeList.add(new Attribute("centerIndex", Attribute.Type.INT));
        attributeList.add(new Attribute("centerDistance", Attribute.Type.DOUBLE));
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
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> map) {

    }

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

}
