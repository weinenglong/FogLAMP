#include <gtest/gtest.h>
#include <configuration_manager.h>
#include <rapidjson/document.h>

using namespace std;
using namespace rapidjson;

TEST(ConfigurationManagerTest, getAllCategoryNames)
{
	// Before the test start the storage layer with FOGLAMP_DATA=.
	// TCP port will be 8080
	ConfigurationManager *cfgManager = ConfigurationManager::getInstance("127.0.0.1", 8080);

	ConfigCategories allCats = cfgManager->getAllCategoryNames();

	string result = "{\"categories\": " + allCats.toJSON() + "}";

	Document doc;
	doc.Parse(result.c_str());

	if (doc.HasParseError() || !doc.HasMember("categories"))
	{
		ASSERT_FALSE(1);
	}

	Value& categories = doc["categories"];

	ASSERT_TRUE(categories.IsArray());

	ConfigCategories confCategories(result);

	ASSERT_EQ(categories.Size(), confCategories.length());
}

TEST(ConfigurationManagerTest, getCategoryItems)
{
	// Before the test start the storage layer with FOGLAMP_DATA=.
	// TCP port will be 8080
	ConfigurationManager *cfgManager = ConfigurationManager::getInstance("127.0.0.1", 8080);
	ConfigCategory category = cfgManager->getCategoryItems("service");
	ASSERT_EQ(0, category.getDescription().compare("FogLAMP Service"));
}
