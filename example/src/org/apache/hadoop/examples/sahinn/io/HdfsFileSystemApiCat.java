package org.apache.hadoop.examples.sahinn.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;

/**
 * @author Sahinn
 * @date 16/5/24
 *
 * 首先是实例化FileSystem对象，通过FileSystem类的get方法，这里要传入一个java.net.URL和一个配置Configuration。
 * 然后FileSystem可以通过一个Path对象打开一个流，之后的操作例子一样
 */
public class HdfsFileSystemApiCat {

    static String ip = "hdfs://172.16.22.251:9005";

    static String path = "/test/input/wc.input";

    public static void main(String[] args) {

        String uri= ip + path;
        Configuration conf=new Configuration();

        InputStream in=null;
        try {
            FileSystem fs= FileSystem.get(URI.create(uri),conf);
            in=fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
