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

import os

from google.appengine.api import validation
from google.appengine.api import yaml_builder
from google.appengine.api import yaml_errors
from google.appengine.api import yaml_listener
from google.appengine.api import yaml_object
from lakshmi import errors

# fetcher_policy.yaml file names
FP_YAML_NAME = "fetcher_policy.yaml"

# stored resource file path 
FP_RESOURCE_PATH = "resource"

# redirect_mode
FOLLOW_ALL = "follow_all"
FOLLOW_NONE = "follow_none"

# minimum response rate
NO_MIN_RESPONSE_RATE = 0

# default setting for content size
DEFAULT_MAX_CONTENT_SIZE = 64 * 1024
# default setting for crawl delay  
DEFAULT_CRAWL_DELAY = 3000
# default setting for max urls per set.
DEFAULT_MAX_URLS_PER_SET = 256
# default setting for urls per skipped set.
URLS_PER_SKIPPED_SET = 100

# N.B. Sadly, we currently don't have and ability to determine
# application root dir at run time. We need to walk up the directory structure
# to find it.
def find_fetcher_policy_yaml(conf_file=__file__):
  """Traverse directory trees to find fetcher_policy.yaml file.

  Begins with the location of configuration.py and then moves on to check the working
  directory.

  Args:
    status_file: location of configuration.py, overridable for testing purposes.

  Returns:
    the path of fetcher_policy.yaml file or None if not found.
  """
  checked = set()
  yaml = _find_fetcher_policy_yaml(os.path.dirname(conf_file), checked)
  if not yaml:
    yaml = _find_fetcher_policy_yaml(os.getcwd(), checked)
  return yaml


def _find_fetcher_policy_yaml(start, checked):
  """Traverse the directory tree identified by start until a directory already
  in checked is encountered or the path of fetcher_policy.yaml is found.

  Checked is present both to make loop termination easy to reason about and so
  that the same directories do not get rechecked.

  Args:
    start: the path to start in and work upward from
    checked: the set of already examined directories

  Returns:
    the path of fetcher_policy.yaml file or None if not found.
  """
  dir = start
  while dir not in checked:
    checked.add(dir)
    yaml_path = os.path.join(dir, FP_YAML_NAME)
    if os.path.exists(yaml_path):
      return yaml_path
    dir = os.path.dirname(dir)
  return None

def parse_fetcher_policy_yaml(contents):
  """Parses fetcher_policy.yaml file contents.

  Args:
    contents: fetcher_policy.yaml file contents.

  Returns:
    FetcherPolicyYaml object with all the data from original file.

  Raises:
    errors.BadYamlError: when contents is not a valid fetcher_policy.yaml file.
  """
  try:
    builder = yaml_object.ObjectBuilder(FetcherPolicyYaml)
    handler = yaml_builder.BuilderHandler(builder)
    listener = yaml_listener.EventListener(handler)
    listener.Parse(contents)

    fp_info = handler.GetResults()
  except (ValueError, yaml_errors.EventError), e:
    raise errors.BadYamlError(e)

  if len(fp_info) < 1:
    raise errors.BadYamlError("No configs found in fetcher_policy.yaml")
  if len(fp_info) > 1:
    raise errors.MultipleDocumentsInFpYaml("Found %d YAML documents" %
                                           len(fp_info))

  jobs = fp_info[0]

  return jobs

class BadConfigurationParameterError(Exception):
  """A parameter passed to a status handler was invalid."""

class MaxContentSizeInfo(validation.Validated):
  """A parameter to max content-size of mime_type. """
  
  ATTRIBUTES = {
    "content_type": r".+",
    "size": "[0-9]+"
  }
  
class FetcherPolicyInfo(validation.Validated):
  """Configuration parameters for the fetcher_policy part of the job."""
  
  ATTRIBUTES = {
    "agent_name": r".+",
    "email_address": r".+",
    "web_address": r".+",
    "min_response_rate": "[0-9]+",
    "max_content_size": validation.Optional(validation.Repeated(MaxContentSizeInfo)),
    "crawl_end_time": "[0-9]+",
    "crawl_delay": "[0-9]+",
    "max_redirects": "[0-9]+",
    "accept_language": r".+",
    "valid_mime_types": r".+",
    "redirect_mode": validation.Options(FOLLOW_ALL,
                                FOLLOW_NONE,
                                default=FOLLOW_ALL),
    "request_timeout": r".+",
    "max_links_per_page": "[0-9]+"
  }
 
class FetcherPolicyYaml(validation.Validated):
  """Root class for fetcher_policy.yaml.

  File format:

  fetcher_policy:
    agent_name: test
    email_address: test@domain.com
    web_address: http://test.domain.com
    min_response_rate: 0
    max_content_size: 65536
    crawl_end_time: 15000
    crawl_delay: 0
    max_redirects: 20
    accept_language: en-us,en-gb,en;q=0.7,*;q=0.3
    valid_mime_types: None
    redirect_mode: follow
    request_timeout: 20000
    max_links_per_page: 10

  Where
    fetcher_policy: The fetcher policy root.
    agent_name: agent_name for fetcher job.
    email_address: email-address of fetcher.
    web_address: web_address of fetcher.
    min_response_rate: rate of http response.
    max_content_size: maximum size of content.
    crawl_end_time: crawl duration you know exactly when the crawl will end.
    crawl_delay: crawl delay time of fetch.
    max_redirects: maximum count of redirects.
    accept_language: restricts the set of natural languages
        that are preferred as a response to the request.
    valid_mime_types: mime types you want to restrict what content type
    redirect_mode
      properties:
        FOLLOW_ALL: Fetcher will try to follow all redirects
        FOLLOW_TEMP: Temp redirects are automatically followed, but not pemanent.
        FOLLOW_NONE: No redirects are followed.
    request_timeout: timeout fetch job.
    max_links_per_page: The max number of extract links per page.
  """

  ATTRIBUTES = {
      "fetcher_policy": validation.Optional(FetcherPolicyInfo)
  }
  
  @classmethod
  def create_default_policy(cls):
    path = os.path.join(os.path.dirname(__file__), FP_RESOURCE_PATH, FP_YAML_NAME)
    fetcher_policy_yaml = parse_fetcher_policy_yaml(open(path))
    return fetcher_policy_yaml
  
  @staticmethod
  def to_dict(fetcher_policy_yaml):
    """Converts a FetcherPolicyYaml file into a JSON-encodable dictionary.

    For use in user-visible UI and internal methods for interfacing with
    user code (like param validation). as a list

    Args:
      fetcher_policy_yaml: The python representation of the fetch_policy_yaml.yaml document.

    Returns:
      A list of configuration dictionaries.
    """
    fetcher_policy = fetcher_policy_yaml.fetcher_policy
    out = {
      "agent_name": fetcher_policy.agent_name,
      "email_address": fetcher_policy.email_address,
      "web_address": fetcher_policy.web_address,
      "min_response_rate": fetcher_policy.min_response_rate,
      "crawl_end_time": fetcher_policy.crawl_end_time,
      "crawl_delay": fetcher_policy.crawl_delay,
      "max_redirects": fetcher_policy.max_redirects,
      "accept_language": fetcher_policy.accept_language,
      "valid_mime_types": fetcher_policy.valid_mime_types,
      "redirect_mode": fetcher_policy.redirect_mode,
      "request_timeout": fetcher_policy.request_timeout,
      "max_links_per_page": fetcher_policy.max_links_per_page
    }
    max_content_sizes = fetcher_policy_yaml.fetcher_policy.max_content_size
    if max_content_sizes:
      max_content_size_list = []
      for param in max_content_sizes:
        params = {}
        params["content_type"] = param.content_type
        params["size"] = param.size
        max_content_size_list.append(params)
      out["max_content_size"] = max_content_size_list

    return out

SC_RESOURCE_PATH="resource"
SC_YAML_NAME="score_config.yaml"

def find_score_config_yaml(conf_file=__file__):
  """Traverse directory trees to find score_config.yaml file.

  Begins with the location of configuration.py and then moves on to check the working
  directory.

  Args:
    status_file: location of configuration.py, overridable for testing purposes.

  Returns:
    the path of score_config.yaml file or None if not found.
  """
  checked = set()
  yaml = _find_score_config_yaml(os.path.dirname(conf_file), checked)
  if not yaml:
    yaml = _find_score_config_yaml(os.getcwd(), checked)
  return yaml

def _find_score_config_yaml(start, checked):
  """Traverse the directory tree identified by start until a directory already
  in checked is encountered or the path of score_config.yaml is found.

  Checked is present both to make loop termination easy to reason about and so
  that the same directories do not get rechecked.

  Args:
    start: the path to start in and work upward from
    checked: the set of already examined directories

  Returns:
    the path of fetcher_policy.yaml file or None if not found.
  """
  dir = start
  while dir not in checked:
    checked.add(dir)
    yaml_path = os.path.join(dir, SC_YAML_NAME)
    if os.path.exists(yaml_path):
      return yaml_path
    dir = os.path.dirname(dir)
  return None

def parse_score_config_yaml(contents):
  """Parses score_config.yaml file contents.

  Args:
    contents: score_config.yaml file contents.

  Returns:
    ScoreConfigYaml object with all the data from original file.

  Raises:
    errors.BadYamlError: when contents is not a valid score_config.yaml file.
  """
  try:
    builder = yaml_object.ObjectBuilder(ScoreConfigYaml)
    handler = yaml_builder.BuilderHandler(builder)
    listener = yaml_listener.EventListener(handler)
    listener.Parse(contents)

    fp_info = handler.GetResults()
  except (ValueError, yaml_errors.EventError), e:
    raise errors.BadYamlError(e)

  if len(fp_info) < 1:
    raise errors.BadYamlError("No configs found in score_config.yaml")
  if len(fp_info) > 1:
    raise errors.MultipleDocumentsInFpYaml("Found %d YAML documents" %
                                           len(fp_info))

  jobs = fp_info[0]

  return jobs

class ScoreConfigInfo(validation.Validated):
  """Configuration parameters for the score_config"""
  ATTRIBUTES = {
      "score_query": r".+",
      "adopt_score": r"[0-9.]+"
  }

class ScoreConfigYaml(validation.Validated):
  """Root class for score_config.yaml.

  File format:

  score_config:
    score_query: Python GoogleAppEngine
    adopt_score: 0.5

  Where
    fetcher_policy: The page scorering configration root.
    score_query: Specified some word of topics
    adopt_socre: Lagger than this value,Adopt to next object for next fetchjob.
  """

  ATTRIBUTES = {
      "score_config": validation.Optional(ScoreConfigInfo)
  }
  
  @classmethod
  def create_default_config(cls):
    path = os.path.join(os.path.dirname(__file__), SC_RESOURCE_PATH, SC_YAML_NAME)
    score_config_yaml = parse_score_config_yaml(open(path))
    return score_config_yaml
  
  @staticmethod
  def to_dict(score_config_yaml):
    """Converts a ScoreConfigYaml file into a JSON-encodable dictionary.

    For use in user-visible UI and internal methods for interfacing with
    user code (like param validation). as a list

    Args:
      score_config_yaml: The python representation of the score_config.yaml document.

    Returns:
      A list of configuration dictionaries.
    """
    score_config = score_config_yaml.score_config
    out = {
      "score_query": score_config.score_query,
      "adopt_score": score_config.adopt_score,
    }

    return out

UF_RESOURCE_PATH="resource"
UF_YAML_NAME="url_filter.yaml"

def find_url_filter_yaml(conf_file=__file__):
  """Traverse directory trees to find url_filter.yaml file.

  Begins with the location of configuration.py and then moves on to check the working
  directory.

  Args:
    status_file: location of configuration.py, overridable for testing purposes.

  Returns:
    the path of url_filter.yaml file or None if not found.
  """
  checked = set()
  yaml = _find_url_filter_yaml(os.path.dirname(conf_file), checked)
  if not yaml:
    yaml = _find_url_filter_yaml(os.getcwd(), checked)
  return yaml

def _find_url_filter_yaml(start, checked):
  """Traverse the directory tree identified by start until a directory already
  in checked is encountered or the path of url_filter.yaml is found.

  Checked is present both to make loop termination easy to reason about and so
  that the same directories do not get rechecked.

  Args:
    start: the path to start in and work upward from
    checked: the set of already examined directories

  Returns:
    the path of url_filter.yaml file or None if not found.
  """
  dir = start
  while dir not in checked:
    checked.add(dir)
    yaml_path = os.path.join(dir, UF_YAML_NAME)
    if os.path.exists(yaml_path):
      return yaml_path
    dir = os.path.dirname(dir)
  return None

def parse_url_filter_yaml(contents):
  """Parses url_filter.yaml file contents.

  Args:
    contents: url_filter.yaml file contents.

  Returns:
    UrlFilterYaml object with all the data from original file.

  Raises:
    errors.BadYamlError: when contents is not a valid url_filter.yaml file.
  """
  try:
    builder = yaml_object.ObjectBuilder(UrlFilterYaml)
    handler = yaml_builder.BuilderHandler(builder)
    listener = yaml_listener.EventListener(handler)
    listener.Parse(contents)

    fp_info = handler.GetResults()
  except (ValueError, yaml_errors.EventError), e:
    raise errors.BadYamlError(e)

  if len(fp_info) < 1:
    raise errors.BadYamlError("No configs found in url_filter.yaml")
  if len(fp_info) > 1:
    raise errors.MultipleDocumentsInFpYaml("Found %d YAML documents" %
                                           len(fp_info))

  jobs = fp_info[0]

  return jobs

class UrlFilterYaml(validation.Validated):
  """Root class for url_filter.yaml.

  File format:

  domain_urlfilter:
    - http://hoge.com
    
  Where
    domain_urlfilter: The filter target url list root.
  """

  ATTRIBUTES = {
      "domain_urlfilter": validation.Optional(list)
  }
  
  @classmethod
  def create_default_urlfilter(cls):
    path = os.path.join(os.path.dirname(__file__), SC_RESOURCE_PATH, UF_YAML_NAME)
    url_filter_yaml = parse_url_filter_yaml(open(path))
    return url_filter_yaml
  
  @staticmethod
  def to_dict(url_filter_yaml):
    """Converts a UrlFilterYaml file into a JSON-encodable dictionary.

    For use in user-visible UI and internal methods for interfacing with
    user code (like param validation). as a list

    Args:
      url_filter_yaml: The python representation of the url_filter.yaml document.

    Returns:
      A list of configuration dictionaries.
    """
    out = {
      "domain_urlfilter": url_filter_yaml.domain_urlfilter,
    }

    return out
