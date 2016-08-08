package org.apache.hadoop.examples.sahinn.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * @author Sahinn
 * @date 16/5/24
 * 和普通文件系统一样，也支持apend操作，写日志时最常用
 * 但并非所有hadoop文件系统都支持append，hdfs支持，s3就不支持。
 * 以下是个拷贝本地文件到hdfs的例子。
 */
public class HdfsFileAppend {

    static String ip = "hdfs://172.16.22.251:9005";

    //权限需要放开
    static String path = "/test/output/wc.output";

    public static void main(String[] args) throws Exception{



        String localSrc = "Test HdfsFileAppend";

        String dst = ip + path;

        InputStream in = new ByteArrayInputStream(localSrc.getBytes());

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096, true);
    }

}
