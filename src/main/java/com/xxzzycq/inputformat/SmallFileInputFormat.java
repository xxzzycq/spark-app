package com.xxzzycq.inputformat;

import com.xxzzycq.recordreader.SmallFileRecordReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by yangchangqi on 2018/4/11.
 */
public class SmallFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        SmallFileRecordReader recordReader = new SmallFileRecordReader();
        recordReader.initialize(inputSplit, taskAttemptContext);
        return recordReader;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        //设置每个文件不能分片，保证一个文件生成一个key-value对
        return false;
    }
}
