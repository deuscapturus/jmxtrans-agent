package org.jmxtrans.agent;

import org.jmxtrans.agent.AbstractOutputWriter;
import org.jmxtrans.agent.util.ConfigurationUtils;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collection;
import java.io.IOException;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
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
        client = CloudWatchClient.builder().build();
        configNamespace = ConfigurationUtils.getString(settings, "namespace", "JMX");
        configDimensions = ConfigurationUtils.getString(settings, "dimensions", "");
        /* Use use Tag class to extract dimensions.  I don't know if Tag is in the right scope to work here. */
        listOfDimensions = Tag.tagsFromCommaSeparatedString(configDimensions);

        for(Tag thisDimension : listOfDimensions) {
            dimensions.add(Dimension.builder().name(thisDimension.getName()).value(thisDimension.getValue()).build());
        }
    }

    @Override
    public void writeQueryResult(String name, String type, Object value) {
        /* Do not use variable 'type'.  It is only set with a "invocation" (instead of a "query"), but the documentation only mentions "query" */
        Double doubleValue;

        if (value instanceof Number) {
            doubleValue = (Double) value;
        } else {
            return;
        }

        try {
            MetricDatum datum = MetricDatum.builder()
                .metricName(name)
                .unit(StandardUnit.NONE)
                .value(doubleValue)
                .dimensions(dimensions).build();

            PutMetricDataRequest request = PutMetricDataRequest.builder()
                .namespace(configNamespace)
                .metricData(datum).build();

            client.putMetricData(request);

        } catch (CloudWatchException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
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
