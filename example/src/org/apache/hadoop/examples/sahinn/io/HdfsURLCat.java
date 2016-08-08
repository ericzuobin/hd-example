package org.apache.hadoop.examples.sahinn.io;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

/**
 * @author Sahinn
 * @date 16/5/24
 *
 * 比较简单的读取hdfs数据的方法就是通过java.net.URL打开一个流，
 * 不过在这之前先要预先调用它的setURLStreamHandlerFactory方法设置为
 * FsUrlStreamHandlerFactory（由此工厂取解析hdfs协议），
 * 这个方法只能调用一次，所以要写在静态块中。
 * 然后调用IOUtils类的copyBytes将hdfs数据流拷贝到标准输出流System.out中，
 * copyBytes前两个参数好理解，一个输入，一个输出，第三个是缓存大小，第四个指定拷贝完毕后是否关闭流。
 * 我们这里要设置为false，标准输出流不关闭，我们要手动关闭输入流。
 */
public class HdfsURLCat {

    static {
        //一个JVM只能调用一次,所以在静态方法中设置
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    static String ip = "hdfs://172.16.22.251:9005";

    static String path = "/test/input/wc.input";

    public static void main(String[] args) {

        InputStream in = null;

        try {
            in = new URL(ip + path).openStream();

            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
