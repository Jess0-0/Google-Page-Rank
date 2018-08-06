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

            //input format: from \t to1, to2, to3
            //output: from to = prob
            String[] fromTos = value.toString().trim().split("\t");
            if (fromTos.length == 1 || fromTos[1].trim().equals("")) {
                return;
            }
            String from = fromTos[0];
            String[] tos = fromTos[1].split(",");
            for (String to : tos) {
                context.write(new Text(from), new Text(to + "=" + (double) 1 / tos.length));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //input format: Page\t 1
            //output: write to reducer
            String[] prID = value.toString().trim().split("\t");
            context.write(new Text(prID[0]), new Text(prID[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {
        float beta; //solve dead ends and spider traps

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //output: unit multiplication
            List<String> transitionCell = new ArrayList<String>();
            double prCell = 0.0;
            for (Text value : values) {
                if (value.toString().contains("=")) {
                    transitionCell.add(value.toString());
                } else {
                    prCell = Double.parseDouble(value.toString());
                }
            }
            for (String cell : transitionCell) {
                String outputKey = cell.split("=")[0];
                double relation = Double.parseDouble(cell.split("=")[1]);

                String outputValue = String.valueOf(relation * prCell * (1 - beta)); //subPr
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //chain two mapper class
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
