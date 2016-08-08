package org.apache.hadoop.examples.sahinn.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @author Sahinn
 * @date 16/5/24
 */
public class HdgsFileListApi {

    static String ip = "hdfs://172.16.22.251:9005/";

    public static void main(String[] args) throws Exception{

        String uri = ip;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        Path[] paths = new Path[1];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = new Path(ip);
        }

        FileStatus[] status = fs.listStatus(paths);
        Path[] listedPaths = FileUtil.stat2Paths(status);
        for (Path p : listedPaths) {
            System.out.println(p);
        }
    }
}
