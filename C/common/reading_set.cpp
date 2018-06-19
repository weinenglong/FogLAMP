/*
 * FogLAMP storage service client
 *
 * Copyright (c) 2018 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch, Massimiliano Pinto
 */
#include <reading_set.h>
#include <string>
#include <rapidjson/document.h>
#include <sstream>
#include <iostream>
#include <time.h>
#include <stdlib.h>

using namespace std;
using namespace rapidjson;

/**
 * Construct a reading set from a JSON document returned from
 * the FogLAMP storage service.
 */
ReadingSet::ReadingSet(const std::string& json)
{
	Document doc;
	doc.Parse(json.c_str());
	if (doc.HasParseError())
	{
		throw new ReadingSetException("Unable to parse results json document");
	}
	if (doc.HasMember("count"))
	{
		m_count = doc["count"].GetUint();
		if (m_count)
		{
			if (!doc.HasMember("rows"))
			{
				throw new ReadingSetException("Missing readings array");
			}
			const Value& readings = doc["rows"];
			if (readings.IsArray())
			{
				unsigned long id = 0;
				// Process every rows and create the result set
				for (auto& reading : readings.GetArray())
				{
					if (!reading.IsObject())
					{
						throw new ReadingSetException("Expected reading to be an object");
					}
					JSONReading *value = new JSONReading(reading);
					m_readings.push_back(value);

					// Get the Reading Id
					id = value->getId();

				}
				// Set the last id
				m_last_id = id;
			}
			else
			{
				throw new ReadingSetException("Expected array of rows in result set");
			}
		}
		else
		{
			m_last_id = 0;
		}
	}
	else
	{
		m_count = 0;
		m_last_id = 0;
	}
}

/**
 * Destructor for a result set
 */
ReadingSet::~ReadingSet()
{
	/* Delete the readings */
	for (auto it = m_readings.cbegin(); it != m_readings.cend(); it++)
	{
		delete *it;
	}
}

/**
 * Convert an ASCII timestamp into a timeval structure
 */
static void convert_timestamp(const char *str, struct timeval *tv)
{
struct tm tm;

	memset(&tm, 0, sizeof(tm));
	strptime(str, "%Y-%m-%d %H:%M:%S", &tm);

    // mktime handles timezones, so UTC is configured
	setenv("TZ","UTC",1);
	tv->tv_sec = mktime(&tm);

	// Work out the microseconds from the fractional part of the seconds
	char fractional[10];
	sscanf(str, "%*d-%*d-%*d %*d:%*d:%*d.%[0-9]*", fractional);
	int multiplier = 6 - strlen(fractional);
	if (multiplier < 0)
		multiplier = 0;
	while (multiplier--)
		strcat(fractional, "0");
	tv->tv_usec = atol(fractional);
}

/**
 * Construct a reading from a JSON document
 *
 * @param json	The JSON document that contains the reading
 */
JSONReading::JSONReading(const Value& json)
{
	m_id = json["id"].GetUint();
	m_has_id = true;
	m_asset = json["asset_code"].GetString();
	convert_timestamp(json["ts"].GetString(), &m_timestamp);
	convert_timestamp(json["user_ts"].GetString(), &m_userTimestamp);
	m_uuid = json["read_key"].GetString();

	// Add 'reading' values
	for (auto& m : json["reading"].GetObject())
	{
		switch (m.value.GetType())
		{
			// String
			case (kStringType):
			{
				DatapointValue value(m.value.GetString());
				this->addDatapoint(new Datapoint(m.name.GetString(),
								 value));
				break;
			}

			// Number
			case (kNumberType):
			{
				if (m.value.IsInt() ||
				    m.value.IsUint() ||
				    m.value.IsInt64() ||
				    m.value.IsUint64())
				{
					DatapointValue value(m.value.GetInt());
					this->addDatapoint(new Datapoint(m.name.GetString(),
									 value));
					break;
				}
				else if (m.value.IsDouble())
				{
					DatapointValue value(m.value.GetDouble());
					this->addDatapoint(new Datapoint(m.name.GetString(),
									 value));
					break;
				}
				else
				{
					string errMsg = "Cannot parse the numeric type";
					errMsg += " of reading element '";
					errMsg.append(m.name.GetString());
					errMsg += "'";

					throw new ReadingSetException(errMsg.c_str());
					break;
				}
			}

			default:
			{
				string errMsg = "Cannot handle unsupported type '" + m.value.GetType();
				errMsg += "' of reading element '";
				errMsg.append(m.name.GetString());
				errMsg += "'";

				throw new ReadingSetException(errMsg.c_str());

				break;
			}
		}
	}
}
