import java.io.*;

public class FlumeEventProducerJava {
    public static void main(String[] args) throws Exception {
        String TrafficDataHome = "/root/Traffic/data/";
        String filePath = TrafficDataHome + "in/2018082013_all_column_test.log";
        String outPath = TrafficDataHome + "out/2018082013_all_column_test.log";


        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outPath)));

        System.out.println("开始生产数据");
        String line;
        while ((line = reader.readLine()) != null) {
            writer.write(line);
            writer.newLine();
            writer.flush();
            Thread.sleep(200);
        }
        writer.close();
        reader.close();


    }
}
