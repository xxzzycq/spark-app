package com.xxzzycq.example.mapreduce;

import com.xxzzycq.bean.FlowBean;
import com.xxzzycq.partitioner.ProvincePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by yangchangqi on 2018/4/10.
 */
public class FlowCount {
    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 拆分一行数据为多个字段
            String[] fields = value.toString().split(",");
            String mobile = fields[0];
            long upFlow = Long.parseLong(fields[1]);
            long downFlow = Long.parseLong(fields[2]);
            context.write(new Text(mobile), new FlowBean(upFlow, downFlow));
        }
    }

    static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context)
                throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_downFlow = 0;
            for (FlowBean flowBean : values) {
                sum_upFlow += flowBean.getUpFlow();
                sum_downFlow += flowBean.getDownFlow();
            }
            FlowBean flowBean = new FlowBean(sum_upFlow, sum_downFlow);
            context.write(key, flowBean);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //指定本程序jar包所在的本地路径
        job.setJarByClass(FlowCount.class);

        //指定job运行的Map和Reduce业务类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //指定自定义的分区器
        job.setPartitionerClass(ProvincePartitioner.class);
        //同时指定相应 "分区" 数量的reduceTask的个数
        job.setNumReduceTasks(5);

        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定最终输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定job原始输入的hdfs目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job最终输出的hdfs目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //将job中配置的相关参数以及job所用的jar包，提交给yarn上运行
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
