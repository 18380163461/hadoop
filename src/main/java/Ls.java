import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class Ls {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void getfileStr(String filePath, String check) {
        final String INNER_DELIMITER = " ";
        // 遍历目录下的所有文件
        BufferedReader br = null;
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(filePath));//获取文件目录
            System.out.println("find result:");
            int cnt = 0;
            for (FileStatus file : status)//遍历文件目录，file为指针
            {
                FSDataInputStream inputStream = fs.open(file.getPath());//输入流 获取某个文件到路径
                br = new BufferedReader(new InputStreamReader(inputStream));//建立 字符缓冲输入流 实例

                String line = null;
                boolean flag = false;
                while (null != (line = br.readLine())) {
                    String[] strs = line.split(INNER_DELIMITER);//设置读取分割符
                    for (int i = 0; i < strs.length; i++) {
//						System.out.println(strs[i]+"-------------");
                        if (strs[i].equals(check)) {
                            flag = true;
                            break;
                        }
                    }
                    if (flag) {
                        cnt++;
                        System.out.println(cnt + "." + file.getPath().getName());
                        break;
                    }
                }// end of while
            }
            if (cnt == 0) System.out.println("No such file!");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        String uri = "hdfs://192.168.93.131:9000/input";
//        String uri=args[0];//接收路径参数
//        String checkstr=args[1];//接收查找字符串参数
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.93.131:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            URI url = new URI("hdfs://192.168.93.131:9000");
            URI url2 = new URI("hdfs://192.168.93.131:9000/input/input/test1.txt");

            try {
                FileSystem fs = FileSystem.get(url, conf, "root");
                Path p = new Path(url2);
                FileStatus fstat = fs.getFileStatus(p);
                System.out.println("文件名称：" + p.getName());//part-r-00000
                System.out.println("系统一个块大小：" + fstat.getBlockSize());//134217728=128M
                System.out.println("文件访问时间：" + fstat.getAccessTime());//1514452493609
                System.out.println("文件整体目录：" + fstat.getPath());//文件目录：hdfs://mrcj:9000/test/output/part-r-00000
                System.out.println("文件大小：" + fstat.getLen());//10777=文件大小
                System.out.println("文件所属用户：" + fstat.getOwner());//hdfs
                System.out.println("是否是目录：" + fstat.isDirectory());//false
                System.out.println("是否是文件：" + fstat.isFile());//true

                System.out.println("");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
//        getfileStr(uri,"checkstr");
    }
}
