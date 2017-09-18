#include "client_http.hpp"
#include "server_http.hpp"
#include "storage_api.h"


// Added for the default_resource example
#include <algorithm>
#include <fstream>
#include <vector>
#ifdef HAVE_OPENSSL
#include "crypto.hpp"
#endif

/**
 * Definition of the Storage Service REST API
 */

StorageApi *StorageApi::m_instance = 0;

using namespace std;
using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;
using HttpClient = SimpleWeb::Client<SimpleWeb::HTTP>;

/**
 * The following are a set of wrapper C functions that are registered with the HTTP Server
 * for each of the API entry poitns. These must be outside if a class as the library has no
 * mechanism to have a class isntance and hence can not provide a "this" pointer for the callback.
 *
 * These functions do the minumum work needed to find the singleton instance of the StorageAPI
 * class and call the appriopriate method of that class to the the actual work.
 */

/**
 * Wrapper function for the common insert API call.
 */
void commonInsertWrapper(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
  StorageApi *api = StorageApi::getInstance();
  api->commonInsert(response, request);
}

/**
 * Wrapper function for the common update API call.
 */
void commonUpdateWrapper(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
  StorageApi *api = StorageApi::getInstance();
  api->commonUpdate(response, request);
}

/**
 * Wrapper function for the common delete API call.
 */
void commonDeleteWrapper(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
  StorageApi *api = StorageApi::getInstance();
  api->commonDelete(response, request);
}

/**
 * Wrapper function for the common simle query API call.
 */
void commonSimpleQueryWrapper(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
  StorageApi *api = StorageApi::getInstance();
  api->commonSimpleQuery(response, request);
}

/**
 * Wrapper function for the common query API call.
 */
void commonQueryWrapper(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
  StorageApi *api = StorageApi::getInstance();
  api->commonQuery(response, request);
}

/**
 * Wrapper function for the default resource API call. This is called whenever
 * an unrecognised API call is received.
 */
void defaultWrapper(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
  StorageApi *api = StorageApi::getInstance();
  api->defaultResource(response, request);
}

/**
 * Called when an error occurs
 */
void on_error(__attribute__((unused)) shared_ptr<HttpServer::Request> request, __attribute__((unused)) const SimpleWeb::error_code &ec) {
}

/**
 * Wrapper function for the reading appendAPI call.
 */
void readingAppendWrapper(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
  StorageApi *api = StorageApi::getInstance();
  api->readingAppend(response, request);
}

/**
 * Wrapper function for the reading fetch API call.
 */
void readingFetchWrapper(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
  StorageApi *api = StorageApi::getInstance();
  api->readingFetch(response, request);
}

/**
 * Wrapper function for the reading query API call.
 */
void readingQueryWrapper(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
  StorageApi *api = StorageApi::getInstance();
  api->readingQuery(response, request);
}

/**
 * Wrapper function for the reading purge API call.
 */
void readingPurgeWrapper(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
  StorageApi *api = StorageApi::getInstance();
  api->readingPurge(response, request);
}


/**
 * Construct the singleton Storage API 
 */
StorageApi::StorageApi(const short port, const int threads) {

	m_port = port;
	m_threads = threads;
	m_server = new HttpServer();
	m_server->config.port = 8080;
	StorageApi::m_instance = this;
}

/**
 * Return the singleton instance of the StorageAPI class
 */
StorageApi *StorageApi::getInstance()
{
  if (m_instance == NULL)
    m_instance = new StorageApi(8080, 1);
  return m_instance;
}

/**
 * Initialise the API entry points for the common data resource and
 * the readings resource.
 */
void StorageApi::initResources()
{
	m_server->resource[COMMON_ACCESS]["POST"] = commonInsertWrapper;
	m_server->resource[COMMON_ACCESS]["GET"] = commonSimpleQueryWrapper;
	m_server->resource[COMMON_QUERY]["GET"] = commonQueryWrapper;
	m_server->resource[COMMON_ACCESS]["PUT"] = commonUpdateWrapper;
	m_server->resource[COMMON_ACCESS]["DELETE"] = commonDeleteWrapper;
	m_server->default_resource["POST"] = defaultWrapper;
	m_server->default_resource["PUT"] = defaultWrapper;
	m_server->default_resource["GET"] = defaultWrapper;
	m_server->default_resource["DELETE"] = defaultWrapper;
	m_server->resource[READING_ACCESS]["POST"] = readingAppendWrapper;
	m_server->resource[READING_ACCESS]["GET"] = readingFetchWrapper;
	m_server->resource[READING_QUERY]["PUT"] = readingQueryWrapper;
	m_server->resource[READING_PURGE]["PUT"] = readingPurgeWrapper;

	m_server->on_error = on_error;
}

/**
 * Start the HTTP server
 */
void StorageApi::start() {
	m_server->start();
}

/**
 * Wait for the HTTP server to shutdown
 */
void StorageApi::wait() {
	m_thread.join();
}

/**
 * Construct an HTTP response with the 200 OK return code using the payload
 * provided.
 *
 * @param response The response stream to send the response on
 * @param payload  The payload to send
 */
void StorageApi::respond(shared_ptr<HttpServer::Response> response, const string& payload)
{
	*response << "HTTP/1.1 200 OK\r\nContent-Length: " << payload.length() << "\r\n"
		 <<  "Content-type: application/json\r\n\r\n" << payload;
}


/**
 * Construct an HTTP response with the specified return code using the payload
 * provided.
 *
 * @param response 	The response stream to send the response on
 * @param code		The HTTP esponse code to send
 * @param payload  	The payload to send
 */
void StorageApi::respond(shared_ptr<HttpServer::Response> response, SimpleWeb::StatusCode code, const string& payload)
{
	*response << "HTTP/1.1 " << status_code(code) << "\r\nContent-Length: " << payload.length() << "\r\n"
		 <<  "Content-type: application/json\r\n\r\n" << payload;
}

/**
 * Perform an insert into a table of the data provided in the payload.
 *
 * @param response	The response stream to send the response on
 * @param request	The HTTP request
 */
void StorageApi::commonInsert(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
string  tableName;
string	payload;

  try {
	  tableName = request->path_match[TABLE_NAME_COMPONENT];
	  payload = request->content.string();

	  stringstream responsePayload;
	  responsePayload << "CommonInsert to table: " << tableName << " payload " << payload << "\n";

	  respond(response, responsePayload.str());
  } catch (exception ex) {
    internalError(response, ex);
  }
}

/**
 * Perform an update on a table of the data provided in the payload.
 *
 * @param response	The response stream to send the response on
 * @param request	The HTTP request
 */
void StorageApi::commonUpdate(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
string  tableName;
string	payload;

  try {
	  tableName = request->path_match[TABLE_NAME_COMPONENT];
	  payload = request->content.string();

	  respond(response, payload);
  } catch (exception ex) {
    internalError(response, ex);
  }
}

/**
 * Perform a simple query on the table using the query parameters as conditions
 *
 * @param response	The response stream to send the response on
 * @param request	The HTTP request
 */
void StorageApi::commonSimpleQuery(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
string  tableName;
string	payload;

  try {
  	tableName = request->path_match[TABLE_NAME_COMPONENT];
  	payload = request->content.string();

	  respond(response, payload);
  } catch (exception ex) {
    internalError(response, ex);
  }
}

/**
 * Perform query on a table using thw JSON encoded query in the payload
 *
 * @param response	The response stream to send the response on
 * @param request	The HTTP request
 */
void StorageApi::commonQuery(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
string  tableName;
string	payload;

  try {
    tableName = request->path_match[TABLE_NAME_COMPONENT];
    payload = request->content.string();

    respond(response, payload);
  } catch (exception ex) {
    internalError(response, ex);
  }
}

/**
 * Perform a delete on a table using the condition encoded in the JSON payload
 *
 * @param response	The response stream to send the response on
 * @param request	The HTTP request
 */
void StorageApi::commonDelete(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
string  tableName;
string	payload;

  try {
	  tableName = request->path_match[TABLE_NAME_COMPONENT];
	  payload = request->content.string();

	  respond(response, payload);
  } catch (exception ex) {
    internalError(response, ex);
  }
}

/**
 * Perform an append operation on the readings.
 *
 * @param response	The response stream to send the response on
 * @param request	The HTTP request
 */
void StorageApi::readingAppend(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
string payload;

  try {
	  payload = request->content.string();

	  respond(response, payload);
  } catch (exception ex) {
    internalError(response, ex);
  }
}

/**
 * Fetch a block of readings.
 *
 * @param response	The response stream to send the response on
 * @param request	The HTTP request
 */
void StorageApi::readingFetch(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
SimpleWeb::CaseInsensitiveMultimap query;
unsigned long id = 0;
unsigned long count = 0;

  try {
    query = request->parse_query_string();

    auto search = query.find("id");
    if (search == query.end())
    {
	    string payload = "{ \"error\" : \"Missing query parameter id\" }";
    	respond(response, SimpleWeb::StatusCode::client_error_bad_request, payload);
      return;
    }
    else
    {
      id = (unsigned)atol(search->second.c_str());
    }
    search = query.find("count");
    if (search == query.end())
    {
	    string payload = "{ \"error\" : \"Missing query parameter count\" }";
    	respond(response, SimpleWeb::StatusCode::client_error_bad_request, payload);
      return;
    }
    else
    {
      count = (unsigned)atol(search->second.c_str());
    }

	  respond(response, "{ \"Fetching...\" : \"data\" }");
  } catch (exception ex) {
    internalError(response, ex);
  }
}

/**
 * Perform a query on a set of readings
 *
 * @param response	The response stream to send the response on
 * @param request	The HTTP request
 */
void StorageApi::readingQuery(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
string	payload;

  try {
    payload = request->content.string();

    respond(response, payload);
  } catch (exception ex) {
    internalError(response, ex);
  }
}


/**
 * Purge the readings
 *
 * @param response	The response stream to send the response on
 * @param request	The HTTP request
 */
void StorageApi::readingPurge(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
SimpleWeb::CaseInsensitiveMultimap query;
unsigned long age = 0;
unsigned long lastSent = 0;
string        flags;

  try {
    query = request->parse_query_string();

    auto search = query.find("age");
    if (search == query.end())
    {
	    string payload = "{ \"error\" : \"Missing query parameter age\" }";
    	respond(response, SimpleWeb::StatusCode::client_error_bad_request, payload);
      return;
    }
    else
    {
      age = (unsigned)atol(search->second.c_str());
    }
    search = query.find("sent");
    if (search == query.end())
    {
	    string payload = "{ \"error\" : \"Missing query parameter sent\" }";
    	respond(response, SimpleWeb::StatusCode::client_error_bad_request, payload);
      return;
    }
    else
    {
      lastSent = (unsigned)atol(search->second.c_str());
    }

    search = query.find("flags");
    if (search != query.end())
    {
      flags = search->second;
    }

	  respond(response, "Purging...");
  } catch (exception ex) {
    internalError(response, ex);
  }
}


/**
 * Handle a bad URL endpoint call
 */
void StorageApi::defaultResource(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request)
{
string	payload;

	payload = "{ \"error\" : \"Unsupported URL: " + request->path + "\" }";
	respond(response, SimpleWeb::StatusCode::client_error_bad_request, payload);
}

/**
 * Handle a exception by sendign back an internal error
 */
void StorageApi::internalError(shared_ptr<HttpServer::Response> response, const exception& ex)
{
const string payload = ex.what();

  respond(response, SimpleWeb::StatusCode::server_error_internal_server_error, payload);
}
