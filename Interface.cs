
using Confluent.Kafka;
using System;
using System.Reflection;
using WEBJOB;
namespace WEBJOB
{
	public class MessageComparer
{
	public enum ComparisonStatus
	{
		Match = 0,  // All properties match
		Unmatch = 1 // At least one property doesn't match
	}

	public ComparisonStatus CompareMessages(BidMessage bid, TradeMessage trade)
	{
		if (bid == null || trade == null)
		{
			throw new ArgumentNullException("Both messages must be non-null for comparison.");
		}

		// Define the property mapping
		var propertyMappings = new (string bidProperty, string tradeProperty)[]
		{
			("BidLoanAmount", "LnBal"),
			("BidSellPricePercent", "FnlPrc"),
			("NoteInterestRate", "NtRt"),
			("MaturityTerm", "Term"),
			("BidAcceptanceDate", "TrdDt")
		};

		// Compare properties using reflection
		foreach (var (bidProperty, tradeProperty) in propertyMappings)
		{
			var bidValue = GetPropertyValue(bid, bidProperty);
			var tradeValue = GetPropertyValue(trade, tradeProperty);

			if (!AreValuesEqual(bidValue, tradeValue))
			{
				return ComparisonStatus.Unmatch;
			}
		}

		return ComparisonStatus.Match;
	}

	private object GetPropertyValue(object obj, string propertyName)
	{
		return obj.GetType().GetProperty(propertyName)?.GetValue(obj);
	}

	private bool AreValuesEqual(object value1, object value2)
	{
		if (value1 == null && value2 == null)
		{
			return true;
		}

		if (value1 == null || value2 == null)
		{
			return false;
		}

		return value1.Equals(value2);
	}
}


	public interface IKafkaProducer : IDisposable
	{
		Task ProduceAsync(string topic, string value, Headers headers);
	}
}
