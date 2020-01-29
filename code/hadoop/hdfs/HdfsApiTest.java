package com.blackdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

public class HdfsApiTest {
    FileSystem fileSystem;

    @Before
    public void setUp()throws Exception{
        URI uri = new URI("hdfs://39.107.239.135:9000");
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname","true");
        configuration.set("dfs.replication","1");
        fileSystem = FileSystem.get(uri, configuration, "black");

    }
    @Test
    public void mkdir()throws Exception{
        fileSystem.mkdirs(new Path("/hdfs1"));
    }

    @Test
    public void copyFromLocalFile ()throws Exception{
        Path src = new Path("input/black");
        Path dst = new Path("/hdfs");
        fileSystem.copyFromLocalFile(src,dst);
    }
    //将hdfs上文件的内容复制到本地所指定的文件中，必须指定本地路径和存储数据的文件名，delsrc参数是个boolean值，表示是否删除源数据
    @Test
    public void copyToLocalFile()throws Exception{
        Path src = new Path("/hdfs/black");
        Path dst = new Path("output/black.txt");
        fileSystem.copyToLocalFile(true,src,dst);
    }
    //将hdfs上文件剪切至目标位置并重命名
    @Test
    public void rename()throws Exception{
        Path src = new Path("/hdfs/black");
        Path dst = new Path("/hdfs1/blackrename");
        fileSystem.rename(src,dst);
    }
    //获取目录下的文件信息，常用于文件合并等场景
    @Test
    public void listFiles()throws Exception{
        //获取文件状态，因为文件可能多个，所以返回迭代器
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfs1/"), true);
        while (files.hasNext()){
            //获取其中一个文件的状态信息
            LocatedFileStatus fileStatus = files.next();
            short replication = fileStatus.getReplication();
            long accessTime = fileStatus.getAccessTime();
            FsPermission permission = fileStatus.getPermission();
            long len = fileStatus.getLen();
            Path path = fileStatus.getPath();
            boolean directory = fileStatus.isDirectory();
            System.out.println(replication + "---" + accessTime + "---" + permission + "---" + len + "---" + path);
            //获取文件的块信息，因为一个文件可能不止一个块存储，因此返回的是数组
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            //遍历数组
            for (BlockLocation location:blockLocations){
                //获取块所在主机信息，之所以是数组，是因为块可能存在副本
                String[] hosts = location.getHosts();
                for (String host:hosts){
                    System.out.println(host);
                }
            }
        }
    }
    //删除hdfs上目标位置文件
    @Test
    public void delete()throws Exception{
        fileSystem.delete(new Path("/hdfs1/black"),true);
    }

    @After
    public void tearDown()throws Exception{
        fileSystem.close();
    }
}
