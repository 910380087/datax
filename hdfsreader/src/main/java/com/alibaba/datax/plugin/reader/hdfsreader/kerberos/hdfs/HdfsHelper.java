package com.alibaba.datax.plugin.reader.hdfsreader.kerberos.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**
 * This is a App to test Hadoop basic file function
 *
 */
public class HdfsHelper 
{
	
	
//	private static String SOURCE_PATH="C:\\Users\\li910\\Desktop\\upload\\job.txt";
//	private static String SOURCE_DOWNLOAD_PATH="C:\\Users\\li910\\Desktop\\upload\\job1.txt";
//	private static String DEST_PATH = "/lishun/job.txt";
	private static String MASTER_URI = "hdfs://47.105.109.232:9000";
	
	
	/**
	 * 本地上传文件到hadoop文件系统
	 * @throws Exception
	 */
	public static void uploadFile(String defaultFS,String sourcePath, String destPath) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", defaultFS);
		//该步骤的目的是为了设置上传hadoop文件时,从环境读出的用户名是root,这样可以比避免修改hdfs的文件夹权限为777
		//也可以避免修改系统账号
//		System.setProperty("HADOOP_USER_NAME", "root");
		FileSystem fs = FileSystem.get(configuration);
		FileInputStream in = new FileInputStream(sourcePath);
		FSDataOutputStream out = fs.create(new Path(destPath),new Progressable() {
			
			public void progress() {
				// TODO Auto-generated method stub
				System.out.println("上传完一个设定缓存区大小容量的文件");
			}
		});
		IOUtils.copyBytes(in, out, 8192,true);
		fs.close();
		in.close();
		out.close();
		System.out.println( "upload success!" );
	}
	
	
	/**
	 * 本地上传文件到hadoop文件系统
	 * @throws Exception
	 */
	public static void uploadFile(Configuration configuration,String sourcePath, String destPath) throws Exception {
//		Configuration configuration = new Configuration();
//		configuration.set("fs.defaultFS", defaultFS);
		//该步骤的目的是为了设置上传hadoop文件时,从环境读出的用户名是root,这样可以比避免修改hdfs的文件夹权限为777
		//也可以避免修改系统账号
//		System.setProperty("HADOOP_USER_NAME", "root");
		FileSystem fs = FileSystem.get(configuration);
		FileInputStream in = new FileInputStream(sourcePath);
		FSDataOutputStream out = fs.create(new Path(destPath),new Progressable() {
			
			public void progress() {
				// TODO Auto-generated method stub
				System.out.println("上传完一个设定缓存区大小容量的文件");
			}
		});
		IOUtils.copyBytes(in, out, 8192,true);
		fs.close();
		in.close();
		out.close();
		System.out.println( "upload success!" );
	}
	
	public static void downloadFile(String defaultFS,String sourcePath, String destPath) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", defaultFS);
		//该步骤的目的是为了设置上传hadoop文件时,从环境读出的用户名是root,这样可以比避免修改hdfs的文件夹权限为777
		//也可以避免修改系统账号
//		System.setProperty("HADOOP_USER_NAME", "root");
		FileSystem fs = FileSystem.get(configuration);
		FSDataInputStream in = fs.open(new Path(sourcePath));
		OutputStream out = new FileOutputStream(destPath);
		IOUtils.copyBytes(in, out, 8192,true);
    	
		System.out.println( "download success!" );
		fs.close();
		in.close();
		out.close();
	}
	
	public static void downloadFile(Configuration configuration,String sourcePath, String destPath) throws Exception {
//		Configuration configuration = new Configuration();
//		configuration.set("fs.defaultFS", defaultFS);
		//该步骤的目的是为了设置上传hadoop文件时,从环境读出的用户名是root,这样可以比避免修改hdfs的文件夹权限为777
		//也可以避免修改系统账号
//		System.setProperty("HADOOP_USER_NAME", "root");
		FileSystem fs = FileSystem.get(configuration);
		FSDataInputStream in = fs.open(new Path(sourcePath));
		OutputStream out = new FileOutputStream(destPath);
		IOUtils.copyBytes(in, out, 8192,true);
    	
		System.out.println( "download success!" );
		fs.close();
		in.close();
		out.close();
	}
	
	public static void deleteHdfsFile(Configuration conf, String defaultFS, String filePath)throws IOException{
		Path path = new Path(filePath);
		conf.set("fs.defaultFS", defaultFS);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(path,true);
		System.out.println("删除文件：" + filePath);
		fs.close();
	}
	
	public static void deleteHdfsFile(Configuration conf, String filePath)throws IOException{
		Path path = new Path(filePath);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(path,true);
		System.out.println("删除文件：" + filePath);
		fs.close();
	}
	
	
	public static void makeHdfsDirs(Configuration conf,String defaultFS, String remoteFile)throws IOException{
//		System.setProperty("HADOOP_USER_NAME", "root");
		conf.set("fs.defaultFS", defaultFS);
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(remoteFile);
		fs.mkdirs(path);
		System.out.println("创建文件夹:" + remoteFile);

	}
	
	public static void makeHdfsDirs(Configuration conf, String remoteFile)throws IOException{
//		System.setProperty("HADOOP_USER_NAME", "root");
//		conf.set("fs.defaultFS", defaultFS);
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(remoteFile);
		fs.mkdirs(path);
		System.out.println("创建文件夹:" + remoteFile);

	}
	
	public static void deleteHdfsDirs(Configuration conf,String defaultFS, String remoteFile)throws IOException{
//		System.setProperty("HADOOP_USER_NAME", "root");
		conf.set("fs.defaultFS", defaultFS);
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(remoteFile);
		fs.delete(path,true);
		System.out.println("删除文件夹:" + remoteFile);

	}
	
	
	public static void deleteHdfsDirs(Configuration conf, String remoteFile)throws IOException{
//		System.setProperty("HADOOP_USER_NAME", "root");
//		conf.set("fs.defaultFS", defaultFS);
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(remoteFile);
		fs.delete(path,true);
		System.out.println("删除文件夹:" + remoteFile);

	}
	
	
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        try {
        	uploadFile(MASTER_URI,"/tmp/test","D:\\a.txt");  	
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println( "upload failed!" );
		}
        try {
			downloadFile(MASTER_URI,"/tmp/test","D:\\a.txt");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println( "download failed!" );
		}
        try {
			HdfsHelper.deleteHdfsDirs(new Configuration(), MASTER_URI, "/upload");			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println( "mkdirs failed!" );
		}
        try {
        	HdfsHelper.makeHdfsDirs(new Configuration(), MASTER_URI, "/longcheng/haha/test");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println( "mkdirs failed!" );
		}
        try {
        	HdfsHelper.deleteHdfsFile(new Configuration(), MASTER_URI, "/lishun/job.txt");
        } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println( "delete failed!" );
		}
    }
}
