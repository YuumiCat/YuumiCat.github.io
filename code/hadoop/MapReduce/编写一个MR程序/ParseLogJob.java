package com.bigdata.etl.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class ParseLogJob extends Configured implements Tool {
    //日志解析类
    public static Text parseLog(String row)throws ParseException {
        //将传入的数据进行切割
        String[] logPart = StringUtils.split(row, "\u1111");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        //将切割出的第一个字段解析成时间格式
        long timeTag = dateFormat.parse(logPart[0]).getTime();
        String activeName = logPart[1];
        JSONObject bizData = JSON.parseObject(logPart[2]);
        JSONObject logData = new JSONObject();
        //将解析出的字段全部放入json中
        logData.put("timeTag",timeTag);
        logData.put("activeName",activeName);
        logData.putAll(bizData);
        return new Text(logData.toString());
    }

    public static class logMapper extends Mapper<LongWritable, Text, NullWritable,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                Text parsedLog = parseLog(value.toString());
                context.write(null,parsedLog);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
    //Driver类，
    public int run(String[] args) throws Exception {
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
/*        Path input = new Path("input");
        Path output = new Path("output");*/
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        //因为输出路径不能预先存在，因此判断下是否存在，如果存在就删除
        if (fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(ParseLogJob.class);
        job.setJobName("parseLog");
        job.setMapperClass(logMapper.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);
        if (!job.waitForCompletion(true)){
            throw new RuntimeException(job.getJobName()+"failed");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int run = ToolRunner.run(new Configuration(), new ParseLogJob(), args);
        System.exit(run);
    }
}
