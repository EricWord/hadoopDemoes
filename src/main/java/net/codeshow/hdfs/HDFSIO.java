package net.codeshow.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;

/**
 * @Description
 * @Author eric
 * @Version V1.0.0
 * @Date 2020/9/29
 */
public class HDFSIO {


    @Test
    public void putFileToHDFS() throws Exception {

        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://Hadoop04:9000"), conf, "root");

        //2. 获取输入流

        FileInputStream fis = new FileInputStream(new File("/Users/cuiguangsong/my_files/workspace/java/hadoopDemoes/src/test/java/net/codeshow/file/my-new.txt"));

        //3.获取输出流
        FSDataOutputStream fos = fs.create(new Path("/file-0930.txt"));


        //4.流的拷贝
        IOUtils.copyBytes(fis, fos, conf);

        //5.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }
}
