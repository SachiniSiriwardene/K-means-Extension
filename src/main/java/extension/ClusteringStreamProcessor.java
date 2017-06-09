package extension;

import Kmeans.Clusterer;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
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
        description = "TBD",
        examples = @Example(description = "TBD", syntax = "TBD")
)
public class ClusteringStreamProcessor extends StreamProcessor implements SchedulingProcessor {
    private static Clusterer clusterer;
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
                data.clear();
                continue;
            } else if (streamEvent.getType() == ComplexEvent.Type.EXPIRED) {
                data.remove(0);
                continue;
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

        Object clustersObject = attributeExpressionExecutors[1].execute(null);
        clusters = (Integer) clustersObject;

        clusterer.setNoOfClusters(clusters);
        ArrayList<Double>[] clusterGroup = new ArrayList[clusters];
        clusterer.setClusterGroup(clusterGroup);

        Object iterationsObject = attributeExpressionExecutors[2].execute(null);
        iterations = (Integer) iterationsObject;
        clusterer.setMaxIter(iterations);

        Object eventsToTrainObject = attributeExpressionExecutors[3].execute(null);
        eventsToTrain = (Integer) eventsToTrainObject;

        Object continueTrainObject = attributeExpressionExecutors[4].execute(null);
        continueTraining = (Boolean) continueTrainObject;

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