package com.twitter.twadoop.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSetRecordReader extends RecordReader<Path, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(FileSetRecordReader.class);

  // Save the split for later use.
  protected FileSetInputSplit split_;
  // Have we given up the single file yet?
  protected boolean finished_ = false;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    split_ = (FileSetInputSplit)inputSplit;

    LOG.info("Creating FileSetRecordReader with InputSplit [{}]", split_.getPath());
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // There is only one k-v pair from each split, so only return true once.
    boolean result = !finished_;
    finished_ = true;
    return result;
  }

  @Override
  public Path getCurrentKey() throws IOException, InterruptedException {
    return split_.getPath();
  }

  @Override
  public NullWritable getCurrentValue() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return finished_ ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {}
}
