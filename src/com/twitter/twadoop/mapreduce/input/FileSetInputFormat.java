package com.twitter.twadoop.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSetInputFormat extends InputFormat<Path, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(FileSetInputFormat.class);

  private static final Set<Path> pathSet_ = Sets.newLinkedHashSet();

  public static void addPath(Path p) {
    pathSet_.add(p);
  }

  public static void addAllPaths(Collection<Path> paths) {
    pathSet_.addAll(paths);
  }

  public static void clearPaths() {
    pathSet_.clear();
  }

  @Override
  public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
    return Lists.transform(Lists.newArrayList(pathSet_), new Function<Path, InputSplit>() {
      public InputSplit apply(Path p) {
        return new FileSetInputSplit(p, jobContext.getConfiguration());
      }
    });
  }

  @Override
  public RecordReader<Path, NullWritable> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new FileSetRecordReader();
  }
}
