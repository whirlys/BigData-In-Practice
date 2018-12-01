import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Vector;

/**
 * @program: hadoopjoin
 * @description:
 * @author: 赖键锋
 * @create: 2018-12-02 00:56
 **/
public class LeftJoin extends Configured implements Tool {

    public static final String DELIMITER = ",";

    public static class LeftJoinMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            /*
             * 拿到两个不同文件，区分出到底是哪个文件，然后分别输出
             */
            String filepath = ((FileSplit) context.getInputSplit()).getPath().toString();
            String line = value.toString();
            if (line == null || line.equals("")) {
                return;
            }

            if (filepath.indexOf("employee") != -1) {
                String[] lines = line.split(DELIMITER);
                if (lines.length < 2) {
                    return;
                }

                String company_id = lines[0];
                String employee = lines[1];
                context.write(new Text(company_id), new Text("a:" + employee));
            } else if (filepath.indexOf("salary") != -1) {
                String[] lines = line.split(DELIMITER);
                if (lines.length < 2) {
                    return;
                }

                String company_id = lines[0];
                String salary = lines[1];
                context.write(new Text(company_id), new Text("b:" + salary));
            }
        }
    }

    public static class LeftJoinReduce extends
            Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            Vector<String> vecA = new Vector<String>();
            Vector<String> vecB = new Vector<String>();

            for (Text each_val : values) {
                String each = each_val.toString();
                if (each.startsWith("a:")) {
                    vecA.add(each.substring(2));
                } else if (each.startsWith("b:")) {
                    vecB.add(each.substring(2));
                }
            }

            for (int i = 0; i < vecA.size(); i++) {
                /*
                 * 如果vecB为空的话，将A里的输出，B的位置补null。
                 */
                if (vecB.size() == 0) {
                    context.write(key, new Text(vecA.get(i) + DELIMITER + "null"));
                } else {
                    for (int j = 0; j < vecB.size(); j++) {
                        context.write(key, new Text(vecA.get(i) + DELIMITER + vecB.get(j)));
                    }
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        GenericOptionsParser optionparser = new GenericOptionsParser(conf, args);
        conf = optionparser.getConfiguration();

        Job job = Job.getInstance(conf, "leftjoin");
        job.setJarByClass(LeftJoin.class);
        FileInputFormat.addInputPaths(job, conf.get("input_dir"));
        Path out = new Path(conf.get("output_dir"));
        FileOutputFormat.setOutputPath(job, out);
        job.setNumReduceTasks(conf.getInt("reduce_num", 1));

        job.setMapperClass(LeftJoinMapper.class);
        job.setReducerClass(LeftJoinReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        conf.set("mapred.textoutputformat.separator", ",");

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LeftJoin(), args);
        System.exit(res);
    }

}