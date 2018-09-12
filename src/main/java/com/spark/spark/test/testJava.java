package com.spark.spark.test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 98726 on 2017/3/13.
 */
public class testJava {
    public  static void main(String [] args){
        try {
            BufferedReader br;
            br = new BufferedReader(new InputStreamReader(new FileInputStream("d:/temp/spark/test.txt")));
            List<String> controlList = new ArrayList<>();
            while(br.read()!=-1){

                String readLine = new String(br.readLine().getBytes(),"UTF-8");
                controlList.add(readLine);
            }

            System.out.println(controlList.toString());
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


    }
}
