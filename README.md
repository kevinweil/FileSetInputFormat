An input format for processing individual Path objects one at a time.  Useful for distributing large jobs that are inherently single-machine across the
entire cluster.  Even if they still run on one machine, this way they get given to an under-utilized node at runtime, and there is built-in resilience to 
task failure.

This input format takes a set of paths and produces a separate input split for each one.  If you need to, for example, unzip a collection of five files
in HDFS, that unzipping has to happen on a single machine per file.  But the set of five files can at least all be unzipped on different machines.  Using
this input format lets you process each file as you wish in your mapper.

1. In your main/run method of your Hadoop job driver class, add

<pre><code>
Job job = new Job(new Configuration());
...

job.setInputFormatClass(FileSetInputFormat.class);

FileSetInputFormat.addPath("/some/path/to/a/file");
FileSetInputFormat.addPath("/some/other/path/to/a/file");
// Also see FileSetInputFormat.addAllPaths(Collection<Path> paths);
</code></pre>

2. Then, make your mapper take a Path as the key and a NullWritable as the value:

<pre><code>
public static class MyMapper extends Mapper&lt;Path, NullWritable, ..., ...&gt; {
    protected void map(Path key, NullWritable value, Context context) throws IOException, InterruptedException {
        // Do something with the path, e.g. open it and unzip it to somewhere.
        ...
    }
}
</code></pre>

The Path keys are the same paths passed in to FileSetInputFormat.addPath and FileSetInputFormat.addAllPaths.  Duplicates are stripped, and one
InputSplit is generated per unique Path.  That's it!
