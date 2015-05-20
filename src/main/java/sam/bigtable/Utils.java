package sam.bigtable;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class Utils
{
	public static void createCF(String tableName, String[] cfNames)
	{
		try
		{
			Connection connection = ConnectionFactory.createConnection();
			Admin admin = connection.getAdmin();
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            for(String colFamily : cfNames)
            {
                tableDescriptor.addFamily(new HColumnDescriptor(colFamily));
            }
            connection.close();
		}
		catch(IOException e)
		{
			
		}
	}
	
	public static void listTables()
	{
		try
		{
			Connection connection = ConnectionFactory.createConnection();
			Admin admin = connection.getAdmin();
            HTableDescriptor[] tables;
            tables = admin.listTables();
            for(HTableDescriptor table : tables)
            {
                HColumnDescriptor[] columnFamilies = table.getColumnFamilies();
                String columnFamilyNames = "";
                for (HColumnDescriptor columnFamily : columnFamilies)
                {
                    columnFamilyNames += columnFamily.getNameAsString() + ",";
                }
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
