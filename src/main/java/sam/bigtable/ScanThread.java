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

public class ScanThread extends Thread
{
	private boolean _canRun;
	private Connection _connection;
	private int _scanStart;
	private int _scanEnd;
	private Table _table;
	private String _threadName;
	
	public ScanThread(String threadName, String tableName, int scanStart, int scanEnd)
	{
		try
		{
			this._canRun = true;
			System.out.println("Thread " + threadName + " connecting...");
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
	
	public void run()
	{
		if(!this._canRun)
		{
			return;
		}
		Scan scan = new Scan();
		scan.setFilter(new SingleColumnValueFilter("cf".getBytes(), "col1".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, "value".getBytes()));
		try
		{
			ResultScanner resultScanner = this._table.getScanner(scan);
			for(Result result : resultScanner)
			{
                this.printResult(result);
			}
		}
		catch(IOException e) {}
	}
	
	public void printResult(Result result)
	{
		for (Cell cell : result.listCells())
		{
            String row = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String column = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            long timestamp = cell.getTimestamp();
            System.out.printf("Row: " + row + ", Family: " + family + ", Column: " + column + ", Timestamp: " + timestamp + ", Value: " + value);
        }
	}
}
