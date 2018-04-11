package com.xxzzycq.recordreader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by yangchangqi on 2018/4/11.
 */
public class SmallFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private static final Logger logger = LoggerFactory.getLogger(LineRecordReader.class);
    private FileSplit fileSplit;
    private Configuration conf;
    private BytesWritable value = new BytesWritable();
    private boolean progressed = false;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue()
            throws IOException, InterruptedException {
        if (!progressed) {
            byte[] content = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream in = null;
            try {
                in = fs.open(file);
                IOUtils.readFully(in, content, 0, content.length);
                value.set(content, 0 , content.length);
            } finally {
                IOUtils.closeStream(in);
            }
            progressed = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey()
            throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue()
            throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress()
            throws IOException, InterruptedException {
        return progressed ? 1.0f : 0.0f;
    }

    @Override
    public void close()
            throws IOException {
    }
}
