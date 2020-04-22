package org.jmxtrans.agent;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;


public class CloudWatchOutputWriter extends AbstractOutputWriter {

    CloudWatchClient client = CloudWatchClient.create();
    private final String configNamespace = ConfigurationUtils.getString(settings, "namespace", "JMX");
    private final String configDimensions = ConfigurationUtils.getString(settings, "dimensions", "");
    /* Use use Tag class to extract dimensions.  I don't know if Tag is in the right scope to work here. */
    private final List<Tag> listOfDimensions = Tag.tagsFromCommaSeparatedString(configDimensions);
    private List<Dimension> dimensions = new ArrayList<>();

    for (Tag thisDimension : listOfDimensions) {
        dimensions.add(new Dimension().withName(thisDimension.getName()).withValue(thisDimension.getvalue()));
    }

    @Override
    public void writeQueryResult(@Nonnull String name, @Nullable String type, @Nullable Object value) {
        /* Do not use variable 'type'.  It is only set with a "invocation" (instead of a "query"), but the documentation only mentions "query" */

        if (value instanceof Number) {
            value = value.doubleValue()
        } else {
            return
        }

        try {
            MetricDatum datum = MetricDatum.builder()
                .metricName(name)
                .unit(StandardUnit.NONE)
                .value(value)
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
    public void writeInvocationResult(@Nonnull String invocationName, @Nullable Object value) throws IOException {
        writeQueryResult(invocationName, null, value);
    }

    @Override
    public void preDestroy() {
        super.preDestroy();
        client.close();
    }

}
