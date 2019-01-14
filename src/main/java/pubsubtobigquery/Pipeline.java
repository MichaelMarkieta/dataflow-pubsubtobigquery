package pubsubtobigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.codehaus.jackson.annotate.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pubsubtobigquery.utils.CustomPipelineOptions;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("serial")
public class Pipeline {
    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);

    public class Event {
        @JsonProperty("EventID")
        String EventID;
        @JsonProperty("HomeID")
        String HomeID;
        @JsonProperty("SmartThingsSensorName")
        String SmartThingsSensorName;
        @JsonProperty("State")
        String State;
        @JsonProperty("Label")
        String Label;
        @JsonProperty("HasBeenLabelled")
        Boolean HasBeenLabelled;
        @JsonProperty("EventTime")
        String EventTime;
        @JsonProperty("CreateDate")
        String CreateDate;
        @JsonProperty("ModifyDate")
        String ModifyDate;
    }

    private static TableSchema getSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("EventType").setType("STRING"));
        fields.add(new TableFieldSchema().setName("EventID").setType("STRING"));
        fields.add(new TableFieldSchema().setName("HomeID").setType("STRING"));
        fields.add(new TableFieldSchema().setName("SmartThingsSensorName").setType("STRING"));
        fields.add(new TableFieldSchema().setName("State").setType("STRING"));
        fields.add(new TableFieldSchema().setName("Label").setType("STRING"));
        fields.add(new TableFieldSchema().setName("HasBeenLabelled").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("EventTime").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("CreateDate").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("ModifyDate").setType("TIMESTAMP"));
        return new TableSchema().setFields(fields);
    }

    static class ParseMessages extends DoFn<PubsubMessage, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String eventString = new String(c.element().getPayload());
            LOG.info(eventString);

            JsonObject obj = new JsonParser().parse(eventString).getAsJsonObject();
            Gson gson = new Gson();
            Event eventJson = gson.fromJson(obj, Event.class);

            TableRow record = new TableRow()
                    .set("EventID", eventJson.EventID)
                    .set("HomeID", eventJson.HomeID)
                    .set("SmartThingsSensorName", eventJson.SmartThingsSensorName)
                    .set("State", eventJson.State)
                    .set("Label", eventJson.Label)
                    .set("HasBeenLabelled", eventJson.HasBeenLabelled)
                    .set("EventTime", eventJson.EventTime)
                    .set("CreateDate", eventJson.CreateDate)
                    .set("ModifyDate", eventJson.ModifyDate);

            if (eventString.contains("#door#")) {
                record.set("EventType", "door");
            } else if (eventString.contains("noiseFloor")) {
                record.set("EventType", "noise");
            } else {
                record.set("EventType", "unknown");
            }

            c.output(record);
        }
    }

    public static void main(String[] args) {
        CustomPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
        org.apache.beam.sdk.Pipeline p = org.apache.beam.sdk.Pipeline.create(options);
        options.setStreaming(true);

        p
                .apply("ReadMessages", PubsubIO.readMessages().fromTopic(String.format("projects/%s/topics/%s", options.getProject(), options.getTopic())).withTimestampAttribute("ts"))
                .apply("ParseMessages", ParDo.of(new ParseMessages()))
                .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                        .to((SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination>) element -> {
                            TableRow row = element.getValue();
                            String table = row.get("EventType").toString();
                            String destination = String.format("%s:%s.%s", options.getProject(), options.getTopic(), table);
                            return new TableDestination(destination, destination);
                        })
                        .withSchema(getSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        p
                .run();
    }
}