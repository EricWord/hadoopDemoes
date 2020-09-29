package net.codeshow.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @Description
 * @Author eric
 * @Version V1.0.0
 * @Date 2020/9/29
 */
public class HDFSClient {

    public static void main(String[] args) throws Exception {

        //获取hdfs客户端对象
        Configuration conf = new Configuration();
        //设置NameNode的地址
//        conf.set("fs.defaultFS", "hdfs://Hadoop04:9000");
//        FileSystem fs = FileSystem.get(conf);
        FileSystem fs = FileSystem.get(new URI("hdfs://Hadoop04:9000"), conf, "root");
        //在hdfs上创建路径
        Path path = new Path("/0929/dashen666");
        boolean mkdirsSuccess = fs.mkdirs(path);
        //关闭资源
        fs.close();
        if (mkdirsSuccess) {
            System.out.println("创建路径成功");
        } else {
            System.out.println("创建路径失败");
        }


    }
}
