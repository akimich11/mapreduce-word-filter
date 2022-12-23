import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Scanner;
import java.util.*;
import java.util.stream.*;
import java.io.File; 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import java.util.HashSet;
import org.apache.hadoop.mapreduce.InputSplit;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordFilter {

    public static final Log log = LogFactory.getLog(WordFilter.class);

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

        private List<String> searchList;
        private String articleName = new String("");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            searchList = Arrays.asList(context.getConfiguration().get("search").split(","));
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                if (key.get() == 0) {
                    articleName = new String(value.toString());
                } else {
                    String line = value.toString();
                    for (String el : searchList)
                        if (line.trim().equalsIgnoreCase(el)) {
                            context.write(value, new Text(articleName));
                            break;
                        }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class WordReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set articleNames = new HashSet();
            for (Text val : values) {
                articleNames.add(val.toString());
            }
            context.write(new Text(key), new Text(String.join("\t", articleNames)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Scanner sc = new Scanner(new File(args[2]));
        List<String> lines = new ArrayList<String>();
        while (sc.hasNextLine()) {
            lines.add(sc.nextLine().trim());
        }
        System.out.println(lines.toString());
        conf.set("search", String.join(",", lines));

        Job job = Job.getInstance(conf, "word filter");
        job.setJarByClass(WordFilter.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        conf.set("mapred.textoutputformat.separatorText", "\t");
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
