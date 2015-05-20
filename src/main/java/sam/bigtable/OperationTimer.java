package sam.bigtable;

import java.util.Calendar;

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
	
	public boolean isTiming()
	{
		return this.isTiming;
	}
	
	public boolean startTiming()
	{
		if(this.isTiming)
		{
			return false;
		}
		Calendar cStart = Calendar.getInstance();
		this.currentOperationStart = cStart.getTimeInMillis();
		this.isTiming = true;
		return true;
	}
	
	public boolean stopTiming()
	{
		if(!this.isTiming)
		{
			return false;
		}
		this.operationCount++;
		Calendar cEnd = Calendar.getInstance();
		long operationTime = cEnd.getTimeInMillis() - this.currentOperationStart;
		if(operationTime < this.shortestTime || this.shortestTime == -1)
		{
			this.shortestTime = operationTime;
		}
		if(operationTime > this.longestTime || this.longestTime == -1)
		{
			this.longestTime = operationTime;
		}
		this.totalTime += operationTime;
		this.isTiming = false;
		return true;
	}
	
	public long getShortestTime()
	{
		return this.shortestTime;
	}
	
	public long getLongestTime()
	{
		return this.longestTime;
	}
	
	public float getAverageTime()
	{
		return this.totalTime / this.operationCount;
	}
	
	public long getTotalTime()
	{
		return this.totalTime;
	}
	
	public int getOperationCount()
	{
		return this.operationCount;
	}
}
