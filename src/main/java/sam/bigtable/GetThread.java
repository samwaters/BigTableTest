package sam.bigtable;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

/**
 * Worker thread to perform read operations from the cluster
 * @author Sam Waters <sam@samwaters.com>
 * @date 20-05-2015
 */
public class GetThread extends BaseThread
{
	/**
	 * Set up the connection to BigTable
	 * @param threadName Unique name for this thread
	 * @param tableName Table on the cluster to use
	 */
	public GetThread(String threadName, String tableName)
	{
		//Connection logic is handled by BaseThread
		super(threadName, tableName);
	}
	
	/**
	 * Perform the read operations until told otherwise
	 * This thread will be told to shut down by the main thread after 30 seconds 
	 */
	public void run()
	{
		int operationCount = 0;
		//Print status every 5 seconds
		long nextPrintTime = System.currentTimeMillis() + 5000;
		Random r = new Random();
		while(this.canRun)
		{
			//Row key is chosen at random
			String rowKey = this.threadName + "-row-" + r.nextInt(1000000);
			try
			{
				//Perform a read operation
				Result result = this.table.get(new Get(rowKey.getBytes()));
			}
			catch(IOException e) {}
			operationCount++;
			if(System.currentTimeMillis() >= nextPrintTime)
			{
				//Print status
				System.out.println(this.threadName + " : " + (operationCount / 5) + " reads/s");
				operationCount = 0;
				nextPrintTime = System.currentTimeMillis() + 5000; 
			}
		}
		//Done, shut down cleanly
		this.closeConnections();
	}
	
	/**
	 * Print the details of a result returned from a read request
	 * @param result The result returned from the read request
	 */
	public void printResult(Result result)
	{
		for (Cell cell : result.listCells())
		{
            String row = new String(CellUtil.cloneRow(cell)); //Row key
            String family = new String(CellUtil.cloneFamily(cell)); //Column family
            String column = new String(CellUtil.cloneQualifier(cell)); //Column name
            String value = new String(CellUtil.cloneValue(cell)); //Value
            long timestamp = cell.getTimestamp(); //Last updated
            System.out.printf("Row: " + row + ", Family: " + family + ", Column: " + column + ", Timestamp: " + timestamp + ", Value: " + value);
        }
	}
}
