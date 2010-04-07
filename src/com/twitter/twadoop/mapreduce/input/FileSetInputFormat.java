package com.twitter.twadoop.mapreduce.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSetInputFormat extends InputFormat<Path, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(FileSetInputFormat.class);

  private static final Joiner commaJoiner_ = Joiner.on(StringUtils.COMMA_STR);
  private static final String CONF_INPUT_KEY = "fileset.input.format.paths";

  /**
   * Add a path to the FileSetInputFormat to be processed in the mapreduce job.
   * @param conf the configuration object
   * @param path the path to add.
   * @throws IOException
   */
  public static void addPath(Configuration conf, Path path) throws IOException {
    addAllPaths(conf, Lists.<Path>newArrayList(path));
  }

  /**
   *
   * Parameters:
   * @param p a file whose contents are files to be added to the paths, one per line.
   * @throws IOException
   */
  public static void addPathsFromSymlinks(Configuration conf, Path p)  throws IOException {
    FileSystem fs = p.getFileSystem(conf);
    for (FileStatus fileStatus : fs.listStatus(p)) {
      if (fileStatus.isDir()) {
        continue;
      }
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
      Set<Path> paths = Sets.newHashSet();
      String line;
      while ((line = reader.readLine()) != null) {
        paths.add(new Path(line));
      }
      addAllPaths(conf, paths);
    }
  }



  /**
   * Add a collection of paths to the FileSetInputFormat to be processed in the mapreduce job.
   * @param conf the configuration object
   * @param paths the paths to add
   * @throws IOException
   */
  public static void addAllPaths(Configuration conf, Collection<Path> paths) throws IOException {
    List<String> filePaths = Lists.newArrayList(getPaths(conf));
    filePaths.addAll(Collections2.transform(paths, new Function<Path, String>() {
      public String apply(Path p) {
        return p.toString();
      }
    }));
    setPaths(conf, filePaths);
  }

  /**
   * Clear the FileSetInputFormat's stored paths.
   * @param conf the configuration object
   */
  public static void clearPaths(Configuration conf) {
    setPaths(conf, Lists.<String>newArrayList());
  }

  /**
   * Get the set of splits from the FileSetInputFormat -- one per file added.
   * @param jobContext the job context object
   * @return a list of splits, one per file added.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    final Configuration conf = jobContext.getConfiguration();
    return Lists.transform(getPaths(conf), new Function<String, InputSplit>() {
      public InputSplit apply(String s) {
        return new FileSetInputSplit(new Path(s), conf);
      }
    });
  }

  /**
   * Create the record reader for the given split and task
   * @param inputSplit the split
   * @param taskAttemptContext the task attempt
   * @return the record reader for this task.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public RecordReader<Path, NullWritable> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new FileSetRecordReader();
  }

  /**
   * Take a list of unescaped input path strings, and set the config variable by combining their
   * escaped versions into a string.
   * @param conf the job configuration
   * @param paths the list of unescaped paths.
   */
  protected static void setPaths(Configuration conf, List<String> paths) {
    List<String> escapedPaths = Lists.transform(paths, new Function<String, String>() {
      public String apply(String s) {
        return StringUtils.escapeString(s);
      }
    });
    Set<String> pathSet = Sets.newLinkedHashSet(escapedPaths);
    conf.set(CONF_INPUT_KEY, commaJoiner_.join(pathSet));
  }

  /**
   * Return the list of unescaped path strings by splitting on the separator and unescaping.
   * @param conf the job configuration
   * @return the list of unescaped paths
   */
  protected static List<String> getPaths(Configuration conf) {
    String pathString = conf.get(CONF_INPUT_KEY, "");
    List<String> paths = Arrays.asList(StringUtils.split(pathString, StringUtils.ESCAPE_CHAR, StringUtils.COMMA));
    return Lists.transform(paths, new Function<String, String>() {
      public String apply(String s) {
        return StringUtils.unEscapeString(s);
      }
    });
  }
}
