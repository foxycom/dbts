package de.uni_passau.dbts.benchmark.utils;

import org.joda.time.DateTime;

/** Time utility. */
public class TimeUtils {

	/**
	 * Converts a datetime string to timestamp in milliseconds.
	 *
	 * @param dateStr Datetime string.
	 * @return Timestamp in milliseconds.
	 */
	public static long convertDateStrToTimestamp(String dateStr){
		DateTime dateTime = new DateTime(dateStr);
		return dateTime.getMillis();
	}
}
