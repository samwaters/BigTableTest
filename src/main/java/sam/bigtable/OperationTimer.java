package sam.bigtable;

/**
 * Class for timing operations
 * Provides methods to get shortest, longest, and total times
 * Can also be used to time multiple operations and get the average time
 * @author Sam Waters <sam@samwaters.com>
 * @date 20-05-2015
 */
public class OperationTimer
{
	private boolean isTiming = false;
	private int operationCount = 0;
	private long shortestTime = -1;
	private long longestTime = -1;
	private long totalTime = 0;
	private long currentOperationStart;
	
	public OperationTimer()
	{
		
	}
	
	/**
	 * Check if an operation is currently being timed
	 * @return Whether an operation is currently being timed
	 */
	public boolean isTiming()
	{
		return this.isTiming;
	}
	
	/**
	 * Start timing an operation
	 * @return true on success, false if an operation is already being timed
	 */
	public boolean startTiming()
	{
		if(this.isTiming)
		{
			return false;
		}
		this.currentOperationStart = System.currentTimeMillis();
		this.isTiming = true;
		return true;
	}
	
	/**
	 * Stop timing an operation
	 * @return true on success, false if no operations are currently being timed
	 */
	public boolean stopTiming()
	{
		if(!this.isTiming)
		{
			return false;
		}
		this.operationCount++;
		long operationTime = System.currentTimeMillis() - this.currentOperationStart;
		//Was this the shortest?
		if(operationTime < this.shortestTime || this.shortestTime == -1)
		{
			this.shortestTime = operationTime;
		}
		//Was this the longest?
		if(operationTime > this.longestTime || this.longestTime == -1)
		{
			this.longestTime = operationTime;
		}
		this.totalTime += operationTime;
		this.isTiming = false;
		return true;
	}
	
	/**
	 * Get the shortest operation time
	 * @return long The shortest operation time
	 */
	public long getShortestTime()
	{
		return this.shortestTime;
	}
	
	/**
	 * Get the longest operation time
	 * @return long The longest operation time
	 */
	public long getLongestTime()
	{
		return this.longestTime;
	}
	
	/**
	 * Get the average operation time
	 * @return float The average operation time
	 */
	public float getAverageTime()
	{
		return this.totalTime / this.operationCount;
	}
	
	/**
	 * Get the total operation time
	 * @return long The total operation time
	 */
	public long getTotalTime()
	{
		return this.totalTime;
	}
	
	/**
	 * Get the amount of operations that have been timed
	 * @return int The operation count
	 */
	public int getOperationCount()
	{
		return this.operationCount;
	}
}
