#ifndef _CONFIGURATION_MANAGER_H
#define _CONFIGURATION_MANAGER_H

/*
 * FogLAMP Configuration management.
 *
 * Copyright (c) 2018 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Massimiliano Pinto
 */

#include <storage_client.h>
#include <config_category.h>
#include <string>

class ConfigurationManager {
        public:
		static ConfigurationManager*	getInstance(const std::string&, short unsigned int);
		ConfigCategories		getAllCategoryNames() const;
		ConfigCategory			getCategoryItems(const std::string& categoryName) const;

	private:
		ConfigurationManager(const std::string& host,
				     unsigned short port);
		~ConfigurationManager();
	private:
		static  ConfigurationManager*	m_instance;
		StorageClient*			m_storage;
};
#endif
