package org.apache.hadoop.examples.sahinn.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;

/**
 * @author Sahinn
 * @date 16/5/24
 * seek方法可跳到文件中的任意位置，我们这里跳到文件的初始位置再重新读一次.
 * 可以在任意位置（第一个参数），偏移量（第三个参数），长度（第四个参数），到数组中（第二个参数）
 */
public class HdfsFSDataInputStreamCat {

    static String ip = "hdfs://172.16.22.251:9005";

    static String path = "/test/input/wc.input";

    public static void main(String[] args) {

        String uri = ip + path;
        Configuration conf = new Configuration();
        FSDataInputStream in=null;
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);


            in.seek(0);
            IOUtils.copyBytes(in, System.out, 4096, false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }

    }
}
