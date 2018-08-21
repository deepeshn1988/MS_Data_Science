package lex.microsoft.com;

import java.util.Scanner;
import java.io.IOException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.Random;

public class App 
{
    static HTable table;

    public static void main( String[] args ) throws IOException
    {
      try
      {
        // Configure the HBase connection
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum",
                   "zookeeper0,zookeeper1,zookeeper2");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.cluster.distributed", "true");
        config.set("zookeeper.znode.parent","/hbase-unsecure");
        
        // Open the HTable
        table = new HTable(config, "Stocks");

        // Read the command-line input until we quit
        System.out.println("\n");
        Scanner scanner = new Scanner(System.in);
        boolean quit = false;
        do
        {
            System.out.println("Enter a stock code, or enter 'quit' to exit.");
            String input = scanner.next();
            // Should we quit?
            if(input.toLowerCase().equals("quit"))
            {
                System.out.println("Quitting!");
                quit = true;
            } else {
                
                // Get the stock data
                getstock(input);

                // Update stock prices
                updatestocks();
            }
        }
        while(!quit);
     }
     catch (Exception ex)
     {
       // Error handling goes here
     }
    }

    public static void getstock( String stock ) throws IOException
    {
        try{
            // Get the stock ticker to search for as a byte array
            byte[] rowId = Bytes.toBytes(stock);
            Get rowData = new Get(rowId);
            // Read the data
            Result result = table.get(rowData);
                    
            // Read the values, converting from byte array to string
            String closingPrice = Bytes.toString(result.getValue(Bytes.toBytes("Closing"),Bytes.toBytes("Price")));
            String currentPrice = Bytes.toString(result.getValue(Bytes.toBytes("Current"),Bytes.toBytes("Price")));
            // Print out the values
            System.out.println("Closing Price: " + closingPrice);
            System.out.println("Current Price: " + currentPrice);
            System.out.println("\n");
        }
        catch (Exception ex)
        {
          // Error handling goes here
        }
    }

    public static void updatestocks() throws IOException
    {
        try{
            // Used to create a new random number
            Random _rand = new Random();
            // Range for random numbers
            Double rangeMin = -1.0;
            Double rangeMax = 1.0;

            // Get all stocks between "AAA" and "ZZZ" in batches of 10
            Scan scan = new Scan(Bytes.toBytes("AAA"), Bytes.toBytes("ZZZ"));
            scan.setBatch(10);
            ResultScanner results = table.getScanner(scan);
            // Iterate over the results
            for(Result result : results)
            {
                String rowId = new String(result.getRow());
                String currentPriceStr = Bytes.toString(result.getValue(Bytes.toBytes("Current"),Bytes.toBytes("Price")));
                // Update current price by random amount between -1 and 1
                Double currentPrice = Double.parseDouble(currentPriceStr);
                Double randomValue = rangeMin + (rangeMax - rangeMin) * _rand.nextDouble();
                currentPrice = currentPrice + randomValue;
                currentPriceStr = String.valueOf(currentPrice);
                Put stockUpdate = new Put(Bytes.toBytes(rowId));
                stockUpdate.add(Bytes.toBytes("Current"), Bytes.toBytes("Price"), Bytes.toBytes(currentPriceStr));
                table.put(stockUpdate);
            }
            // Flush committed updates
            table.flushCommits();
        }
        catch (Exception ex)
        {
          // Error handling goes here
        }
    }
}
