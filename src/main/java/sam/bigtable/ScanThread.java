package sam.bigtable;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

/**
 * Worker thread to perform scan operations on the cluster
 * @author Sam Waters <sam@samwaters.com>
 * @date 20-05-2015
 *
 */
public class ScanThread extends Thread
{
	private boolean _canRun;
	private Connection _connection;
	private int _scanStart;
	private int _scanEnd;
	private Table _table;
	private String _threadName;
	
	/**
	 * Set up the connection to BigTable
	 * @param threadName Unique name for the thread
	 * @param tableName Table to use on the cluster
	 * @param scanStart Scan start value
	 * @param scanEnd Scan end value
	 */
	public ScanThread(String threadName, String tableName, int scanStart, int scanEnd)
	{
		try
		{
			this._canRun = true;
			System.out.println("Thread " + threadName + " connecting...");
			//Connect to BigTable
			this._connection  = ConnectionFactory.createConnection();
			this._table = this._connection.getTable(TableName.valueOf(tableName));
		}
		catch(IOException e)
		{
			System.out.println("Thread " + threadName + " throwing exception: " + e.getMessage());
			this._canRun = false;
		}
		this._threadName = threadName;
		this._scanStart = scanStart;
		this._scanEnd = scanEnd;
	}
	
	/**
	 * Perform scan operation(s)
	 */
	public void run()
	{
		if(!this._canRun)
		{
			return;
		}
		//Build a new scanner
		Scan scan = new Scan();
		//col1 >= value
		//TODO: Use values passed in
		scan.setFilter(new SingleColumnValueFilter("cf".getBytes(), "col1".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, "value".getBytes()));
		try
		{
			//Perform the scan operation
			ResultScanner resultScanner = this._table.getScanner(scan);
			for(Result result : resultScanner)
			{
				//Print the result
                this.printResult(result);
			}
		}
		catch(IOException e) {}
	}
	
	/**
	 * Print details of a result from the scan operation
	 * @param result The result from the scan operation
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
