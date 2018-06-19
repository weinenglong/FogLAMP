#ifndef _MANAGEMENT_CLIENT_H
#define _MANAGEMENT_CLIENT_H
/*
 * FogLAMP storage service.
 *
 * Copyright (c) 2017-2018 OSisoft, LLC
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch, Massimiliano Pinto
 */
#include <client_http.hpp>
#include <config_category.h>
#include <service_record.h>
#include <logger.h>
#include <string>
#include <map>
#include <rapidjson/document.h>

using HttpClient = SimpleWeb::Client<SimpleWeb::HTTP>;
using namespace rapidjson;

class ManagementClient {
	public:
		ManagementClient(const std::string& hostname, const unsigned short port);
		~ManagementClient();
		bool 			registerService(const ServiceRecord& service);
		bool 			unregisterService();
		bool 			getService(ServiceRecord& service);
		bool 			registerCategory(const std::string& categoryName);
		bool 			unregisterCategory(const std::string& categoryName);
		ConfigCategories	getCategories();
		ConfigCategory		getCategory(const std::string& categoryName);

	private:
		HttpClient				*m_client;
		std::string				*m_uuid;
		Logger					*m_logger;
		std::map<std::string, std::string>	m_categories;
	public:
		// member template must be here and not in .cpp file
		template<class T> bool	addCategory(const T& t)
		{
			try {
				std::string url = "/foglamp/service/category";
				auto res = m_client->request("POST", url.c_str(), t.toJSON());
				Document doc;
				std::string response = res->content.string();

				doc.Parse(response.c_str());
				if (doc.HasParseError())
				{
					m_logger->error("Failed to parse result of adding a category: %s\n",
							response.c_str());
					return false;
				}
				else if (doc.HasMember("message"))
				{
					m_logger->error("Failed to add configuration category: %s.",
							doc["message"].GetString());
					return false;
				}
				else
				{
					return true;
				}
			} catch (const SimpleWeb::system_error &e) {
				m_logger->error("Add config category failed %s.", e.what());
			}
			return false;
		};
};

#endif
