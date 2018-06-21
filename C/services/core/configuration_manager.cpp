/*
 * FogLAMP FogLAMP Configuration management.
 *
 * Copyright (c) 2018 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Massimiliano Pinto
 */

#include <configuration_manager.h>
#include <rapidjson/writer.h>

using namespace std;

ConfigurationManager *ConfigurationManager::m_instance = 0;

// Constructor
ConfigurationManager::ConfigurationManager(const string& host,
					   unsigned short port)
{
	m_storage = new StorageClient(host, port);
}

// Destructor
ConfigurationManager::~ConfigurationManager()
{
	delete m_storage;
}

/**
 * Return the singleton instance of the configuration manager
 */
ConfigurationManager* ConfigurationManager::getInstance(const string& host,
							unsigned short port)
{
	if (m_instance == 0)
	{
		m_instance = new ConfigurationManager(host, port);
	}
	return m_instance;
}

// Get all FogLAMP categories
ConfigCategories ConfigurationManager::getAllCategoryNames() const
{
	vector<Returns *> columns;
	columns.push_back(new Returns("key"));
	columns.push_back(new Returns("description"));
	Query qAllCategories(columns);

	// Query via Storage client
	ResultSet* allCategories = m_storage->queryTable("configuration", qAllCategories);

	ConfigCategories categories;

	for (ResultSet::RowIterator it = allCategories->firstRow(); ;)
	{
		ResultSet::Row* row = *it;
		if (!row)
		{
			// TODO: add specific Exception
			throw;
		}	
		ResultSet::ColumnValue* key = row->getColumn("key");
		ResultSet::ColumnValue* description = row->getColumn("description");

		ConfigCategoryDescription *value = new ConfigCategoryDescription(key->getString(),
										 description->getString());

		// Add current row data to categories;
		categories.addCategoryDescription(value);
	
		if (allCategories->isLastRow(it))
		{
			break;
		}

		it++;
	}

	// Deallocate categories object
	delete allCategories;

	return categories;
}

// Get items of a specific category
ConfigCategory ConfigurationManager::getCategoryItems(const string& categoryName) const
{
	// SELECT * FROM foglamp.configuration WHERE key = categoryName
	const Condition conditionKey(Equals);
	Where *wKey = new Where("key", conditionKey, categoryName);
	Query qKey(wKey);

	// Query via storage client
	ResultSet* categoryItems = m_storage->queryTable("configuration", qKey);

	// Get first row
	ResultSet::RowIterator it = categoryItems->firstRow();
	ResultSet::Row* row = *it;
	if (!row)
	{
		// TODO: add specific Exception
		throw;
	}	

	ResultSet::ColumnValue* key = row->getColumn("key");
	ResultSet::ColumnValue* description = row->getColumn("description");
	ResultSet::ColumnValue* items = row->getColumn("value");

	// Create string representation of JSON object
	rapidjson::StringBuffer buffer;
	rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
	const rapidjson::Value *v = items->getJSON();
	v->Accept(writer);

	std::string sItems(buffer.GetString(), buffer.GetSize());

	// Create category oject
	ConfigCategory retVal = ConfigCategory(key->getString(), sItems);

	// Set description
	retVal.setDescription(description->getString());

	// Deallocate category items object
	delete categoryItems;

	return retVal;
}
