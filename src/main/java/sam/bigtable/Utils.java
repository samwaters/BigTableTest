package sam.bigtable;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * Utilities for working with BigTablle
 * @author Sam Waters <sam@samwaters.com>
 * @date 20-05-2015
 */
public class Utils
{
	/**
	 * Create a new column family on the cluster
	 * @param tableName Name of new column family to create
	 * @param cfNames Columns to add to the new family
	 */
	public static void createCF(String tableName, String[] cfNames)
	{
		try
		{
			//Connect to BigTable
			Connection connection = ConnectionFactory.createConnection();
			//Get table descriptor
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            for(String colFamily : cfNames)
            {
            	//Add columns
                tableDescriptor.addFamily(new HColumnDescriptor(colFamily));
            }
            //Create the table
            Admin admin = connection.getAdmin();
            admin.createTable(tableDescriptor);
            connection.close();
		}
		catch(IOException e)
		{
			
		}
	}
	
	/**
	 * List currently available tables and their column families and columns
	 */
	public static void listTables()
	{
		try
		{
			//Connect to BigTable
			Connection connection = ConnectionFactory.createConnection();
			//Get the table list
			Admin admin = connection.getAdmin();
            HTableDescriptor[] tables;
            tables = admin.listTables();
            for(HTableDescriptor table : tables)
            {
            	//Get the column families in this table
                HColumnDescriptor[] columnFamilies = table.getColumnFamilies();
                String columnFamilyNames = "";
                //Get the columns in each column family
                for (HColumnDescriptor columnFamily : columnFamilies)
                {
                    columnFamilyNames += columnFamily.getNameAsString() + ",";
                }
                //If there are column families, print them
                if (columnFamilyNames.length() > 0) 
                {
                    columnFamilyNames = " <" + columnFamilyNames.substring(0, columnFamilyNames.length()) + ">";
                }
                System.out.println(table.getTableName() + columnFamilyNames);
            }
            connection.close();
		}
		catch(IOException e)
		{
			
		}
	}
}
