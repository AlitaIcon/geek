package com.geek.hm.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class PathUtils {
    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (String child : children) {
                if (child == null)continue;
                boolean success = deleteDir(new File(dir, child));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }

    public static Path outputFile(String path){
        File file = new File(path);
        if (file.exists()){
            deleteDir(file);
        }
        return new Path(path);
    }

    public static void isPathExistOrDelete(String path, Boolean delete) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), conf, "root");
        Path path1 = new Path(path);
        boolean exists = fileSystem.exists(path1);
        if (exists && delete){
            fileSystem.delete(path1, true);
        }
    }
}
