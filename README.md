# elastic-search-uploader
elastic-search-uploader

This threading.Thread object has an attribute "data_list". It is a list of dictionaries. User can append dictionaries to it. 
Once its size passes bulksize, the thread will upload data_list to ElasticSearch using elasticsearch.helpers.bulk()

Init Parameters:

	elasticsearch_ip - IP address of ElasticSearch
	port - Port of Elastic Search
	bulksize - Bulk size of the data list. data_list will be uploaded to ElasticSearch once its size passes bulksize
	index - ElasticSearch index for the data uploaded
	log - logging.getLogger object (Optional, will create a default one)
	timeout - ElasticSearch action time out (Optional, default is 600)


