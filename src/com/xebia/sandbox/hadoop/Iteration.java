package com.xebia.sandbox.hadoop;

import com.xebia.sandbox.hadoop.job2.calculate.RankCalculateMapper;
import com.xebia.sandbox.hadoop.job2.calculate.RankCalculateReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class Iteration extends Configured implements Tool {

    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Iteration(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        boolean isCompleted;

        String lastResultPath = null;

        for (int runs = 0; runs < 3; runs++) {
            String inPath = "/wiki/ranking/iter" + nf.format(runs);
            lastResultPath = "/wiki/ranking/iter" + nf.format(runs + 1);

            isCompleted = runRankCalculation(inPath, lastResultPath);

            if (!isCompleted) return 1;
        }

        return 0;
    }

    private boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankCalculator = Job.getInstance(conf, "rankCalculator");
        rankCalculator.setJarByClass(Iteration.class);

        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

        rankCalculator.setMapperClass(RankCalculateMapper.class);
        rankCalculator.setReducerClass(RankCalculateReduce.class);

        rankCalculator.setNumReduceTasks(16);
        
        return rankCalculator.waitForCompletion(true);
    }
}
