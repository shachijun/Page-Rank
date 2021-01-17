import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability
            String[] fromeTos = value.toString().trim().split("\t");
            if (fromeTos.length < 2) {
                return;//面试的时候不要直接return
            }
            String[] tos = fromeTos[1].split(",");
            String outputKey = fromeTos[0];
            for (String to : tos) {
                context.write(new Text(outputKey), new Text(to + "=" + (double) 1/tos.length));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            //target: write to reducer
            String[] idPr = value.toString().trim().split("\t");
            if (idPr.length != 2) {
                return;
            }
            context.write(new Text(idPr[0]), new Text(idPr[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication
            double prcell = 0;
            List<String> tranCell = new ArrayList<String>();
            boolean flag = false;
            for (Text value : values) {
                if (flag || value.toString().contains("=")) {
                    tranCell.add(value.toString());
                } else {
                    prcell = Double.parseDouble(value.toString());
                    flag = true;
                }
            }
            for (String transCell : tranCell) {
                String[] transArr = transCell.split("=");
                String toID = transArr[0];
                double prob = Double.parseDouble(transArr[1]);
                double subPr = prob * prcell;
                context.write(new Text(toID),new Text(String.valueOf(subPr)));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
