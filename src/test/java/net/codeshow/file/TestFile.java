package net.codeshow.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Description
 * @Author eric
 * @Version V1.0.0
 * @Date 2020/9/29
 */
public class TestFile {

    //文件上传
    @Test
    public void testCopyFromLocalFile() throws Exception {
        //1.获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://Hadoop04:9000"), conf, "root");
        //2.执行上传API
        fs.copyFromLocalFile(new Path("/Users/cuiguangsong/my_files/workspace/java/hadoopDemoes/src/test/java/net/codeshow/file/test.txt"), new Path("/my66.txt"));
        //3.关闭资源
        fs.close();
    }

    //文件下载
    @Test
    public void testCopyToLocalFile() throws Exception {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://Hadoop04:9000"), conf, "root");


        //2.执行下载操作
        fs.copyToLocalFile(new Path("/my66.txt"), new Path("/Users/cuiguangsong/my_files/workspace/java/hadoopDemoes/src/test/java/net/codeshow/file/my-new.txt"));


        //3.关闭资源
        fs.close();


    }
}
