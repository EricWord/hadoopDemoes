package net.codeshow.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.net.URI;

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

    //文件删除
    @Test
    public void testDelete() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://Hadoop04:9000"), conf, "root");

        boolean success = fs.delete(new Path("/0929"), true);
        if (success) {
            System.out.println("文件删除成功");
        } else {
            System.out.println("文件删除失败");
        }

        fs.close();

    }

    //文件更改名字
    @Test
    public void testRename() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://Hadoop04:9000"), conf, "root");

        //执行更名操作


        boolean success = fs.rename(new Path("/my.txt"), new Path("/you.txt"));
        if (success) {
            System.out.println("文件更改名字成功");
        } else {
            System.out.println("文件更改名字失败");
        }
        //关闭资源

        fs.close();
    }


    //文件详情查看
    @Test
    public void testListFiles() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://Hadoop04:9000"), conf, "root");


        //查看文件详情
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();


            //文件名称
            System.out.println("文件名称:" + fileStatus.getPath().getName());

            //权限
            System.out.println("文件的权限:" + fileStatus.getPermission());

            //长度
            System.out.println("长度:" + fileStatus.getLen());

            //块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println("块信息:");
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {

                    System.out.println("host=" + host);
                }

            }

            System.out.println("----------一个文件结束---------");


        }
        //关闭资源
        fs.close();
    }

    //文件和文件夹的判断

    @Test
    public void testFileStatus() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://Hadoop04:9000"), conf, "root");

        //判断操作
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件->" + fileStatus.getPath().getName());

            } else {
                System.out.println("文件夹->" + fileStatus.getPath().getName());
            }
        }
        fs.close();


    }
}
