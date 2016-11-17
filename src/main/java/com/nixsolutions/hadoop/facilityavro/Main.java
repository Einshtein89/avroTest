package com.nixsolutions.hadoop.facilityavro;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.nixsolutions.hadoop.model.Facility;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Java action for working with some log file in order to process it and store.
 *
 */
public class Main {
    // private static final File outputFile = new File(
    // outputPath + "facility.avro");

    static Facility facility = new Facility();
    static DatumWriter<Facility> datumWriter = new SpecificDatumWriter<Facility>(
            Facility.class);
    static DataFileWriter<Facility> fileWriter = new DataFileWriter<Facility>(
            datumWriter);

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            usage();
        }
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);
        AppProps.addApplicationTag(properties, "tutorials");
        AppProps.addApplicationTag(properties, "cluster:development");
        AppProps.setApplicationName(properties, "facility");
        Hadoop2MR1FlowConnector flowConnector = new Hadoop2MR1FlowConnector(
                properties);
        // Input file
        String inputPath = args[0];
        // Output file
        // String outputPath = args[1];

        Configuration config = new Configuration();
        config.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
        config.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));

        FileSystem fs = FileSystem.get(config);

        // Path outputtmp = new Path("/sdfsdf/sdfsdf");
        Path fileNamePath = new Path("vasya/facility.avro");
        FSDataOutputStream fsOut = null;
        try {
            if (fs.exists(fileNamePath)) {
                fs.delete(fileNamePath, true);
            }
            // fs.create(outputtmp);
            fsOut = fs.create(fileNamePath, true);
            // FSDataOutputStream fin = fs.create(fileNamePath);
            // fin.writeUTF("hello");
            // fin.writeChars("sdsdf");
            // fin.close();
        } catch (Exception e) {
            // TODO: handle exception
        }

        // create the source tap
        Tap<?, ?, ?> source = new Hfs(new TextLine(), inputPath);

        // Create a sink tap to write to the Hfs; by default, TextDelimited
        // writes all fields out
        Tap<?, ?, ?> sink = new Hfs(new TextDelimited(true, "\t"), "output",
                SinkMode.REPLACE);

        new File(outputPath).mkdir();
        // create the job definition, and run it
        FlowDef flowDef = Main.fileProcessing(source, sink, fsOut);
        Flow wcFlow = flowConnector.connect(flowDef);
        flowDef.setAssertionLevel(AssertionLevel.VALID);
        wcFlow.complete();
        fileWriter.close();
        fsOut.close();
    }

    static void usage() {
        System.out.println(
                "Usage : HadoopDFSFileReadWrite <inputfile> <output file>");
        System.exit(1);
    }

    public static FlowDef fileProcessing(Tap<?, ?, ?> source, Tap<?, ?, ?> sink,
            OutputStream out) throws IOException {

        fileWriter.create(facility.getSchema(), out);
        Pipe pipe = new Each("split", new Fields("line"),
                new FileProcessing(new Fields("line")), Fields.SWAP);

        return FlowDef.flowDef()//
                .addSource(pipe, source) //
                .addTail(pipe)//
                .addSink(pipe, sink);
    }

    public static class FileProcessing extends BaseOperation
            implements Function {

        public FileProcessing(Fields fieldDeclaration) throws IOException {
            super(1, fieldDeclaration);

        }

        @Override
        public void operate(FlowProcess flowProcess,
                FunctionCall functionCall) {
            TupleEntry argument = functionCall.getArguments();
            String line = lineProcessing(argument.getString(0));

            if (line.length() > 0) {
                Tuple result = new Tuple();
                result.add(line);
                functionCall.getOutputCollector().add(result);
            }
        }

        public String lineProcessing(String text) {
            try {
                if (!text.contains("TruvenFacilityName")) {
                    String[] splitString = text.split("\\|");
                    for (int i = 0; i < splitString.length - 4; i++) {
                        facility.setTruvenFacilityName(splitString[i]);
                        facility.setAcoId(splitString[i + 1]);
                        facility.setTruvenFacilityId(splitString[i + 2]);
                        facility.setCDAFacilityId(splitString[i + 3]);
                        facility.setCdaClientCode(splitString[i + 4]);
                        fileWriter.append(facility);
                    }
                }
            } catch (Exception e) {

            }
            return text;
        }
    }
}
