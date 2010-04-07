package com.twitter.twadoop.mapreduce.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSetInputSplit extends InputSplit implements Writable {
  private static final Logger LOG = LoggerFactory.getLogger(FileSetInputSplit.class);

  private Path path_;
  private Configuration conf_;

  public FileSetInputSplit() {
    conf_ = new Configuration();
  }

  public FileSetInputSplit(Path path, Configuration conf) {
    path_ = path;
    conf_ = conf;
  }

  public Path getPath() {
    return path_;
  }
  
  @Override
  public long getLength() throws IOException, InterruptedException {
    return 1;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(conf_);
    FileStatus fileStatus = fs.getFileStatus(path_);
    BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    Set<String> hosts = Sets.newHashSet();
    for (BlockLocation bl: blockLocations) {
      hosts.addAll(Arrays.asList(bl.getHosts()));
    }
    return hosts.toArray(new String[0]);
  }

  public void write(DataOutput output) throws IOException {
    output.writeUTF(path_.toString());
    conf_.write(output);
  }

  public void readFields(DataInput input) throws IOException {
    path_ = new Path(input.readUTF());
    conf_.readFields(input);
  }
}
