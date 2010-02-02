package com.twitter.twadoop.mapreduce.input;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class TestFileSetInputFormat {
  FileSetInputFormat input_;
  JobContext jc_;

  @Before
  public void setup() {
    input_ = new FileSetInputFormat();
    jc_ = new JobContext(new Configuration(), new JobID());
  }

  @Test
  public void testSplits() throws IOException, InterruptedException {
    FileSetInputFormat.clearPaths();
    FileSetInputFormat.addPath(new Path("/my/file/name"));
    FileSetInputFormat.addAllPaths(Lists.<Path>newArrayList(new Path("/another/file/name"),
                                                            new Path("/yet/another/file/name")));
    List<InputSplit> splits = input_.getSplits(jc_);
    assertEquals(splits.size(), 3);

    assertPathEquals(splits.get(0), "/my/file/name");
    assertPathEquals(splits.get(1), "/another/file/name");
    assertPathEquals(splits.get(2), "/yet/another/file/name");
  }

  @Test
  public void testUniqueSplits() throws IOException, InterruptedException {
    FileSetInputFormat.clearPaths();
    FileSetInputFormat.addAllPaths(Lists.<Path>newArrayList(new Path("/another/file/name"),
                                                            new Path("/yet/another/file/name"),
                                                            new Path("/another/file/name")));
    List<InputSplit> splits = input_.getSplits(jc_);
    assertEquals(splits.size(), 2);

    assertPathEquals(splits.get(0), "/another/file/name");
    assertPathEquals(splits.get(1), "/yet/another/file/name");
  }

  private void assertPathEquals(InputSplit split, String str) {
    FileSetInputSplit realSplit = (FileSetInputSplit)split;
    assertEquals(realSplit.getPath().toString(), str);
  }
}
