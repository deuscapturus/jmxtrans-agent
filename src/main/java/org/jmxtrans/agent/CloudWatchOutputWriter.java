package org.jmxtrans.agent;

import org.jmxtrans.agent.AbstractOutputWriter;
import org.jmxtrans.agent.util.ConfigurationUtils;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.io.IOException;

import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;

public class CloudWatchOutputWriter extends AbstractOutputWriter {

    private CloudWatchClient client;
    private String configNamespace;
    private String configDimensions;
    /* Use use Tag class to extract dimensions.  I don't know if Tag is in the right scope to work here. */
    private List<Tag> listOfDimensions;
    private Collection<Dimension> dimensions;

    @Override
    public void postConstruct(Map<String, String> settings) {

        client = CloudWatchClient.create();
        configNamespace = ConfigurationUtils.getString(settings, "namespace", "JMX");
        configDimensions = ConfigurationUtils.getString(settings, "dimensions", "");
        /* Use use Tag class to extract dimensions.  I don't know if Tag is in the right scope to work here. */
        listOfDimensions = Tag.tagsFromCommaSeparatedString(configDimensions);

        logger.log(getInfoLevel(), "listOfDimensions =" + configDimensions
        + ", listOfDimensions=" + listOfDimensions);

        /* FIXME
        for(Tag thisDimension : listOfDimensions) {
            dimensions.add(Dimension.builder().name(thisDimension.getName()).value(thisDimension.getValue()).build());
        }*/
    }

    @Override
    public void writeQueryResult(String name, String type, Object value) {
        /* Do not use variable 'type'.  It is only set with a "invocation" (instead of a "query"), but the documentation only mentions "query" */

        Double doubleValue;

        if (value instanceof Number) {
            if (value instanceof Long) {
                logger.log(Level.WARNING, "Cannot write result " + name + ". " + value + " because it is type Long.  TODO: Figure out how to case a java.lang.Object which is java.lang.Long as a Double.");
                return;
            } else {
                doubleValue = (Double) value;
            }
        } else {
            logger.log(Level.WARNING, "Cannot write result " + name + ". " + value + " is not a Number.");
            return;
        }

        try {

            MetricDatum datum = MetricDatum.builder()
                .metricName(name)
                .value(doubleValue)
                .dimensions(dimensions).build();

            PutMetricDataRequest request = PutMetricDataRequest.builder()
                .namespace(configNamespace)
                .metricData(datum).build();

            logger.log(Level.INFO, "request = " + request);
            PutMetricDataResponse response = client.putMetricData(request);
            logger.log(Level.INFO, "putMetricData response: " + response.toString());

        } catch (CloudWatchException e) {
            logger.log(Level.SEVERE, e.awsErrorDetails().errorMessage());
        }
    }

    @Override
    public void writeInvocationResult(String invocationName, Object value) throws IOException {
        writeQueryResult(invocationName, null, value);
    }

    @Override
    public void preDestroy() {
        super.preDestroy();
        client.close();
    }

}
