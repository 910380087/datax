package com.alibaba.datax.plugin.reader.hdfsreader.kerberos;


import com.alibaba.datax.plugin.reader.hdfsreader.kerberos.login.HuaWeiLoginUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class KerberosHelper {


    /**
     * 华为的hive认证
     * url 示例 example : 10.78.152.62:24002,10.78.152.42:24002,10.78.152.52:24002,10.78.152.24:24002,10.78.152.14:24002/etlpts_hive_db;
     * serviceDiscoveryMode=zooKeeper;
     * zooKeeperNamespace=hiveserver2;
     * sasl.qop=auth-conf;
     * hadoop.security.authentication=kerberos;dacp.kerberos.principal=jc_etlpts;
     * principal=hive/hadoop.hadoop.com@HADOOP.COM;
     * dacp.keytab.file=/app/dacp/user_keytab/user.keytab;
     * dacp.java.security.krb5.conf=/app/dacp/user_keytab/krb5.conf;
     * user.principal=jc_etlpts;
     * zk_principal=zookeeper/hadoop.hadoop.com;
     * zookeeper_login_name=Client;
     * @param jdbcUrl 传入的jdbcurl
     * @return 可用的jdbcurl
     */
    public static String HuaWeiHiveKerberos(String jdbcUrl){
        System.out.println("解析是否开始kerberos认证");
        System.out.println("jdbcUrl内容：" + jdbcUrl);
        String url = jdbcUrl;
        System.out.println("解析URL");
        String[] results = url.split(";");
        //没有后缀直接返回
        if (results.length < 2)
            return jdbcUrl;
        //解析
        Map<String,String> map = new HashMap<String, String>();
        for (int i=1; i<results.length; i++){
            System.out.println("分解信息:" + results[i]);
            String[] mapKeyValue = results[i].split("=");
            map.put(mapKeyValue[0],mapKeyValue[1]);
        }

        //认证变量
        String isKerberos = "kerberos";
        String hadoop_security_authentication;
        String krb5_confpath = "";
        @SuppressWarnings("unused")
		String principal = "";
        org.apache.hadoop.conf.Configuration config;
        String user_principal = "";
        String zookeeper_default_login_context_name = "Client";
        String user_keytab_file = "";
        String zk_server_principal_key = "zookeeper.server.principal";
        String zk_principal = "";


        //遍历map并构造新String
        StringBuilder urlBuilder = new StringBuilder().append(results[0]);
        for (Map.Entry<String,String> entry : map.entrySet()){
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
            urlBuilder.append(";");
            if ("hadoop.security.authentication".equals(entry.getKey())){
                isKerberos = entry.getValue();
                urlBuilder.append("auth").append("=").append(entry.getValue());
            }else if ("principal".equals(entry.getKey())){
                principal = entry.getValue();
                urlBuilder.append("principal").append("=").append(entry.getValue());
            }else if ("dacp.kerberos.principal".equals(entry.getKey())){
                user_principal = entry.getValue();
                urlBuilder.append("user.principal").append("=").append(entry.getValue());
            }else if ("dacp.java.security.krb5.conf".equals(entry.getKey())){
                krb5_confpath = entry.getValue();
            }else if ("dacp.keytab.file".equals(entry.getKey())){
                user_keytab_file = entry.getValue();
                urlBuilder.append("user.keytab").append("=").append(entry.getValue());
            }else if ("zk_principal".equals(entry.getKey())){
                zk_principal = entry.getValue();
            }else if ("zookeeper_login_name".equals(entry.getKey())){
                zookeeper_default_login_context_name = entry.getValue();
            }else {
                urlBuilder.append(entry.getKey()).append("=").append(entry.getValue());
            }
        }
        System.out.println("新的JdbcUrl: " + urlBuilder.toString());


        //华为认证
        if ("kerberos".equalsIgnoreCase(isKerberos)){
            hadoop_security_authentication = "kerberos";
            //设置krb5文件环境变量
            System.setProperty("java.security.krb5.conf", krb5_confpath);
            //设置hadoop一个configuration配置
            config = new org.apache.hadoop.conf.Configuration();
            config.set("hadoop.security.authentication", hadoop_security_authentication);
            //设置JAAS配置
            try {
                HuaWeiLoginUtil.setJaasConf(zookeeper_default_login_context_name, user_principal, user_keytab_file);
                System.out.println("JAAS配置成功");
            } catch (IOException e) {
                e.printStackTrace();
            }
            //zk认证登陆
            if (!"".equals(zk_principal)) {
            	try {
                    HuaWeiLoginUtil.setZookeeperServerPrincipal(zk_server_principal_key, zk_principal);
                    System.out.println("zk认证登陆成功");
                } catch (IOException e) {
                    e.printStackTrace();
                }
			}
            
            //hive认证登陆
            try {
                HuaWeiLoginUtil.login(user_principal, user_keytab_file, krb5_confpath , config);
                System.out.println("hive认证登陆成功");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("认证登陆成功");
        return urlBuilder.toString();
    }




    /**
     * 华为hdfs的kerberos认证
     * @param defaultFS 默认登陆地址
     * @param confJson 配置的json信息
     *      以下是配置信息的示例
     *                 {
     * 	                "dfs.nameservices": "hacluster,ns2,ns1",
     * 	                "dfs.ha.namenodes.hacluster": "2889,2888",
     * 	                "dfs.namenode.rpc-address.hacluster.2889": "pc-zjqbetl11:25000",
     * 	                "dfs.namenode.http-address.hacluster.2889": "pc-zjqbetl11:25002",
     * 	                "dfs.client.failover.proxy.provider.hacluster": "org.apache.hadoop.hdfs.server.namenode.ha.BlackListingFailoverProxyProvider",
     * 	                "dfs.ha.namenodes.ns2": "5261,5262",
     * 	                "dfs.namenode.rpc-address.ns2.5261": "pc-zjqbba869:25000",
     * 	                "dfs.namenode.rpc-address.ns2.5262": "pc-zjqbba870:25000",
     * 	                "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.BlackListingFailoverProxyProvider",
     * 	                "dfs.ha.namenodes.ns1": "5258,5257",
     * 	                "dfs.namenode.rpc-address.ns1.5258": "pc-zjqbba867:25000",
     * 	                "dfs.namenode.rpc-address.ns1.5257": "pc-zjqbba868:25000",
     * 	                "dfs.client.failover.proxy.provider.ns2": "org.apache.hadoop.hdfs.server.namenode.ha.BlackListingFailoverProxyProvider"
     *                  }
     * @param kerberos 是否开启kerberos
     * @param kerberosPrincipal kerberos名
     * @param krb5Conf kerberos的krb5配置文件
     * @param userKeytab kerberos的user键值文件
     * @return
     */
    public static org.apache.hadoop.conf.Configuration HuaWeiHdfsKerberos(String defaultFS,String confJson,
                                                    boolean kerberos, String kerberosPrincipal,
                                                    String krb5Conf, String userKeytab){
        //hadoop 定义
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

        //添加文件系统地址
        hadoopConf.set("fs.defaultFS", defaultFS);

        //遍历配置信息,添加到hadoop configuration
        if (!"".equals(confJson) && confJson != null) {
            JSONObject hadoopSiteParamsAsJsonObject = JSON.parseObject(confJson);
            for (Map.Entry<String, Object> entry : hadoopSiteParamsAsJsonObject.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
                hadoopConf.set(entry.getKey(), entry.getValue().toString());
            }
        }

        //kerberos相关
        if (kerberos) {
            hadoopConf.set("hadoop.security.authentication", "kerberos");

            //华为特需的特性
            hadoopConf.set("hadoop.rpc.protection", "privacy");

            //登陆前设置
            try {
                HuaWeiLoginUtil.setKrb5Config(krb5Conf);
                HuaWeiLoginUtil.setConfiguration(hadoopConf);
            } catch (IOException e) {
                e.printStackTrace();
            }

            //认证登陆
            try {
                System.out.println("kerberos认证开始");
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, userKeytab);
                System.out.println("kerberos认证通过");
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
            }

        }

        return hadoopConf;
    }




}
