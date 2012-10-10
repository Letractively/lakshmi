#!/usr/bin/env python
#
# Copyright 2012 cloudysunny14.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import robotparser
import datetime
import re

from mapreduce.lib.files import file_service_pb

from urlparse import urlparse

from google.appengine.ext import ndb
from google.appengine.ext import blobstore

from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
from mapreduce import input_readers
from mapreduce import mapper_pipeline
from mapreduce.lib import pipeline
from mapreduce.lib import files

from mapreduce.lib.pipeline import common as pipeline_common
from mapreduce import output_writers
from lakshmi import configuration
from lakshmi import fetchers
from lakshmi.datum import CrawlDbDatum
from lakshmi.datum import FetchedDatum

#Define Fetch Status
UNFETCHED, FETCHED, FAILED, SKIPPED = range(4) 

def getDomain(url):
  parsed_uri = urlparse(url)
  return '%s://%s' % (parsed_uri.scheme, parsed_uri.netloc)

def _extact_domain_map(entity_type):
  """Extract domain from url map function."""
  data = ndb.Model.to_dict(entity_type)
  extract_domain = ""
  fetch_status = data.get("last_status", 2) 
  if fetch_status == UNFETCHED:
    url = data.get("url")
    extract_domain = getDomain(url)
    entity_type.extract_domain_url = extract_domain
    entity_type.put()

  yield(extract_domain, "")

def _grouped_domain_reduce(key, values):
  """Grouping url reduce function."""
  cr = ""
  if(len(key)>0):
    cr = "\n"
  yield key + cr
  
class _ExactDomainMapreducePipeline(base_handler.PipelineBase):
  """Pipeline to execute exactDomain to fetch of MapReduce job.
  
  Args:
    job_name: job name as string.
    params: parameters for DatastoreInputReader,
      that params use to CrawlDbDatum.
    shard_count: shard count for mapreduce.
  Returns:
    file_names: output path of exact domains,
      that will generate to urls csv.
  """
  def run(self,
          job_name,
          params,
          shard_count):
    yield mapreduce_pipeline.MapreducePipeline(
        job_name,
        __name__ + "._extact_domain_map",
        __name__ + "._grouped_domain_reduce",
        "mapreduce.input_readers.DatastoreInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
        mapper_params=params,
        reducer_params={
            "mime_type": "text/plain",
        },
        shards=shard_count)

class _RobotsLineInputReader(input_readers.BlobstoreLineInputReader):
  """Reader that for robots fetch map job's files consists from line.
  
  This input reader behaver is same as BlobstoreLineInputReader,
  Override the split_input class method particular for RobotFetchJob.
  """
  
  @classmethod
  def split_input(cls, mapper_spec):
    """Returns a list of shard_count input_spec_shards for input_spec.

    Args:
      mapper_spec: The mapper specification to split from. Must contain
          'blob_keys' parameter with one or more blob keys.

    Returns:
      A list of BlobstoreInputReaders corresponding to the specified shards.
    """
    params = input_readers._get_params(mapper_spec)
    file_names = params[cls.BLOB_KEYS_PARAM]
    if isinstance(file_names, basestring):
      # This is a mechanism to allow multiple filenames (which do not contain
      # commas) in a single string. It may go away.
      file_names = file_names.split(",")

    blob_sizes = {}
    for file_name in file_names:
      blob_key = files.blobstore.get_blob_key(file_name)
      blob_key_str = str(blob_key)
      blob_info = blobstore.BlobInfo.get(blobstore.BlobKey(blob_key_str))
      blob_sizes[blob_key_str] = blob_info.size

    shard_count = min(cls._MAX_SHARD_COUNT, mapper_spec.shard_count)
    shards_per_blob = shard_count // len(file_names)
    if shards_per_blob == 0:
      shards_per_blob = 1

    chunks = []
    for blob_key, blob_size in blob_sizes.items():
      blob_chunk_size = blob_size // shards_per_blob
      for i in xrange(shards_per_blob - 1):
        chunks.append(input_readers.BlobstoreLineInputReader.from_json(
            {cls.BLOB_KEY_PARAM: blob_key,
             cls.INITIAL_POSITION_PARAM: blob_chunk_size * i,
             cls.END_POSITION_PARAM: blob_chunk_size * (i + 1)}))
      chunks.append(input_readers.BlobstoreLineInputReader.from_json(
          {cls.BLOB_KEY_PARAM: blob_key,
           cls.INITIAL_POSITION_PARAM: blob_chunk_size * (shards_per_blob - 1),
           cls.END_POSITION_PARAM: blob_size}))
    return chunks

  @classmethod
  def validate(cls, mapper_spec):
    """Validates mapper spec and all mapper parameters.

    Args:
      mapper_spec: The MapperSpec for this InputReader.

    Raises:
      BadReaderParamsError: required parameters are missing or invalid.
    """
    if mapper_spec.input_reader_class() != cls:
      raise input_readers.BadReaderParamsError("__RobotsLineInputReader:Mapper input reader class mismatch")
    params = input_readers._get_params(mapper_spec)
    if cls.BLOB_KEYS_PARAM not in params:
      raise input_readers.BadReaderParamsError("_RobotsLineInputReader:Must specify 'blob_keys' for mapper input")
    file_names = params[cls.BLOB_KEYS_PARAM]
    if isinstance(file_names, basestring):
      # This is a mechanism to allow multiple blob keys (which do not contain
      # commas) in a single string. It may go away.
      file_names = file_names.split(",")
    if len(file_names) > cls._MAX_BLOB_KEYS_COUNT:
      raise input_readers.BadReaderParamsError("_RobotsLineInputReader:Too many 'blob_keys' for mapper input")
    if not file_names:
      raise input_readers.BadReaderParamsError("_RobotsLineInputReader:No 'blob_keys' specified for mapper input")
    for file_name in file_names:
      blob_key = files.blobstore.get_blob_key(file_name)
      blob_key_str = str(blob_key)
      blob_info = blobstore.BlobInfo.get(blobstore.BlobKey(blob_key_str))
      if not blob_info:
        raise input_readers.BadReaderParamsError("_RobotsLineInputReader:Could not find blobinfo for key %s" %
                                   blob_key_str)

def _robots_fetch_map(data):
  """Map function of fetch robots.txt from page.
  
  Fetch robots.txt from Web Pages in specified url,
  Fetched result content will store to Blobstore,
  which will parse and set the score for urls.
  
  Args:
    data: key value data, that key is position, value is url.
  Returns:
    url: extract domain url.
    content: content of fetched from url's robots.txt
  """
  fetcher_policy_yaml = configuration.FetcherPolicyYaml.create_default_policy()
  fetcher = fetchers.SimpleHttpFetcher(1, fetcher_policy_yaml.fetcher_policy)
  k, url = data
  logging.debug("data"+str(k)+":"+str(url))
  result = fetcher.get("%s/robots.txt" % str(url))
  yield (url, result.get("content"))
  
class _RobotsFetchPipeline(base_handler.PipelineBase):
  """Pipeline to execute RobotFetch jobs.
  
  Args:
    job_name: job name as string.
    blob_keys: files which urls for fetch robots.txt are stored. 
    shards: number of shards.
  Returns:
    file_names: output path of fetch results.
  """
  def run(self,
          job_name,
          blob_keys,
          shards):
    yield mapreduce_pipeline.MapperPipeline(
      job_name,
      __name__ + "._robots_fetch_map",
      __name__ + "._RobotsLineInputReader",
      output_writer_spec=output_writers.__name__ + ".KeyValueBlobstoreOutputWriter" ,
      params={
            "blob_keys": blob_keys,
          },
      shards=shards)

def _makeFetchSetBufferMap(binary_record):
  """Map function of create fetch buffers,
  that output thus is one or more fetch url to fetch or skip.
  
  Arg:
    binary_record: key value data, that key is extract domain url,
      value is content from robots.txt.
  Returns:
    url: to fetch url.
    fetch_or_unfetch: the boolean value of fetch or unfetch,
      if sets true is fetch, false is skip.
  """
  proto = file_service_pb.KeyValue()
  proto.ParseFromString(binary_record)
  extract_domain_url = proto.key()
  content = proto.value()
  
  #Get the fetcher policy from resource.
  fetcher_policy_yaml = configuration.FetcherPolicyYaml.create_default_policy()
  user_agent = fetcher_policy_yaml.fetcher_policy.agent_name
  rp = robotparser.RobotFileParser()
  rp.parse(content.split("\n").__iter__())
  
  #Extract urls from CrawlDbDatum.
  query = CrawlDbDatum.query(CrawlDbDatum.extract_domain_url==extract_domain_url)
  entities = query.fetch()

  for entity in entities:
    url = entity.url
    can_fetch = rp.can_fetch(user_agent, url)
    yield (url, can_fetch)

class _FetchSetsBufferPipeline(base_handler.PipelineBase):
  """Pipeline to execute FetchSetsBuffer jobs.
  
  Args:
    job_name: job name as string.
    file_names: file names of fetch result of robots.txt. 
  Returns:
    file_names: output path of fetch results.
  """
  def run(self,
          job_name,
          file_names):
    yield mapreduce_pipeline.MapperPipeline(
      job_name,
      __name__ + "._makeFetchSetBufferMap",
      "mapreduce.input_readers.RecordsReader",
      output_writer_spec=output_writers.__name__ + ".KeyValueBlobstoreOutputWriter" ,
      params={
            "files": file_names,
          },
      shards=len(file_names))

def _str2bool(v):
  return v.lower() in ("yes", "true", "t", "1")

def _fetchMap(binary_record):
  """Map function of create fetch result,
  that create FetchResulDatum entity, will be store to datastore. 
  Arg:
    binary_record: key value data, that key is url to fetch,
      value is boolean value of can be fetch.
  Returns:
    url: to fetch url.
    fetch_result: the result of fetch.
  """
  proto = file_service_pb.KeyValue()
  proto.ParseFromString(binary_record)
  url = proto.key()
  could_fetch = _str2bool(proto.value())
  result = UNFETCHED
  fetched_url = ""
  #Fetch to CrawlDbDatum
  crawl_db_key = ndb.Key(CrawlDbDatum, url)
  crawl_db_datums = CrawlDbDatum.fetch_crawl_db(crawl_db_key)
  crawl_db_datum = crawl_db_datums[0]
  if could_fetch:
    #start fetch
    fetcher_policy_yaml = configuration.FetcherPolicyYaml.create_default_policy()
    fetcher = fetchers.SimpleHttpFetcher(1, fetcher_policy_yaml.fetcher_policy)
    try:
      fetch_result = fetcher.get(url)
      if fetch_result:
        #Storing to datastore
        fetched_datum = FetchedDatum(
            parent=crawl_db_datum.key,
            url = url,
            fetched_url = fetch_result.get("fetched_url"),
            fetch_time = fetch_result.get("time"),
            content_text = fetch_result.get("content_text"),
            content_binary = fetch_result.get("content_binary"),
            content_type =  fetch_result.get("mime_type"),
            content_size = fetch_result.get("read_rate"),
            response_rate = fetch_result.get("read_rate"),
            http_headers = str(fetch_result.get("headers")))
        fetched_datum.put()
        #update time of last fetched 
        crawl_db_datum.last_fetched = datetime.datetime.now()
        result = FETCHED
        fetched_url = ("%s\n"%url)
    except Exception as e:
      logging.warning("Fetch Error Occurs:" + e.message)
      result = FAILED
  else:
    result = SKIPPED
  
  #update the crawlDbDatum's status
  crawl_db_datum.last_updated = datetime.datetime.now()
  crawl_db_datum.last_status = result
  crawl_db_datum.put()
  
  yield fetched_url

class _FetchPipeline(base_handler.PipelineBase):
  """Pipeline to execute Fetch jobs.
  
  Args:
    job_name: job name as string.
    file_names: file names of fetch result count and status 
    shards: number of shards.
  Returns:
    file_names: output path of fetch results.
  """
  def run(self,
          job_name,
          file_names,
          shards):
    yield mapreduce_pipeline.MapperPipeline(
      job_name,
      __name__ + "._fetchMap",
      "mapreduce.input_readers.RecordsReader",
      output_writer_spec=output_writers.__name__ + ".BlobstoreOutputWriter" ,
      params={
        "files": file_names,
      },
      shards=len(file_names))

class FetcherPipeline(base_handler.PipelineBase):
  """ Pipeline to execute FetchPipeLine jobs.
  
  Args:
    job_name: job name as string.
    params: params for fetch job.
    shards: number of shard for fetch job.
  Returns:
    The list of filenames as string. Resulting files contain serialized
    file_service_pb.KeyValues protocol messages with all values collated
    to a single key.
  """
  def run(self,
          job_name,
          params,
          shards):
    extract_domain_files = yield _ExactDomainMapreducePipeline(job_name,
        params=params,
        shard_count=shards)
    robots_files = yield _RobotsFetchPipeline(job_name, extract_domain_files, shards)
    fetch_set_buffer_files = yield _FetchSetsBufferPipeline(job_name, robots_files)
    result_files = yield _FetchPipeline(job_name, fetch_set_buffer_files, shards)
    yield ExtractOutlinksPipeline(result_files)
    temp_files = [extract_domain_files, robots_files, fetch_set_buffer_files, result_files]
    with pipeline.After(result_files):
      all_temp_files = yield pipeline_common.Extend(*temp_files)
      yield mapper_pipeline._CleanupPipeline(all_temp_files)

class ExtractOutlinksPipeline(base_handler.PipelineBase):
  """ Pipeline to execute ExtractOutlinksPipeline.
    Extract Outlinks from html,
    after the extract job, 
    create the CrawlDbDatum from extracted outlinks url.
    that is object to next fetchjob.

  Args:
    file_names: Input filenames, consists from results of fetch job.
  """
  def run(self,
          file_names):
    for file_name in file_names:
      blob_key = files.blobstore.get_blob_key(file_name)
      blob_reader = blobstore.BlobReader(blob_key)
      url = blob_reader.readline()
      while url:
        entities = CrawlDbDatum.fetch_crawl_db(ndb.Key(CrawlDbDatum, url.rstrip("\n")))
        for entity in entities:
          fetched_datums = FetchedDatum.fetch_fetched_datum(entity.key)
          content = None
          if len(fetched_datums)>0:
            content = fetched_datums[0].content_text

          if content is not None:
            urls = re.findall(r'href=[\'"]?([^\'" >]+)', content)
            crawl_depth = entity.crawl_depth
            crawl_depth += 1
            for url in urls:
              parsed_uri = urlparse(url)
              if parsed_uri.scheme == "http" or parsed_uri.scheme == "https":
                crawl_db_datum = CrawlDbDatum(
                    parent=ndb.Key(CrawlDbDatum, url),
                    url=url,
                    last_status=UNFETCHED,
                    crawl_depth=crawl_depth)
                crawl_db_datum.put()

        url = blob_reader.readline()