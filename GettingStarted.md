# Overview #
The lakshmi distribution comes with some examples that demonstrate how to use the lakshmi web crawling and mining toolkit.(now, appear the crawl demo only. More examples release soon.)

# Getting started with lakshmi #
### 1.Checkout the lakshmi from repository. ###
```
git clone https://code.google.com/p/lakshmi/
```
Archive contents as follows:
```
- demo           : demo application that uses the lakshmi
- src            : python source code for the lakshmi
- test           : tests for the lakshmi
```
### 2.lakshmi library is requires to [Google App Engine mapreduce API](http://code.google.com/p/appengine-mapreduce/) ###
Download mapreduce API as follows.
```
svn checkout http://appengine-mapreduce.googlecode.com/svn/trunk/ appengine-mapreduce-read-only
```
### 3.lakshmi library and mapreduce library into demo folder ###
The lakshmi library is /lakshmi/src/likshmi
<br>The mapreduce library is /trunk/python/mapreduce/src/mapreduce<br>
<br>Into the demo folder as follows:<br>
<pre><code>lakshmi/<br>
|-- demo/<br>
    |-- mapreduce/<br>
    |-- lakshmi/<br>
</code></pre>
<h3>4.Defining a parser function</h3>
All of our examples specify Parser function and start a mapreduce job via the following API call:<br>
<pre><code>pipelines.FetcherPipeline("FetcherPipeline",<br>
    params={<br>
      "entity_kind": ENTITY_KIND<br>
    },<br>
    parser_params={<br>
      "text/html": "main.htmlParser",<br>
      "application/rss+xml": "main.htmlParser",<br>
      "application/atom+xml": "main.htmlParser",<br>
      "text/xml": "main.htmlParser"<br>
    },<br>
    shards=4)<br>
</code></pre>
Fetcher Pipeline via the <a href='http://code.google.com/p/appengine-pipeline/'>Pipeline API</a> with following structure:<br>
1. Job name of fetcher<br>
2.Input kind name for Fetcher Pipeline,which entity is consists by seed urls.see Store the root urls to your datastore.<br>
3.Call the user-supplied parser function<br>
4.Number of shards<br>
<br>
Parser function: Our parser consists of the following code:<br>
<pre><code>def htmlParser(key, content):<br>
  outlinks = re.findall(r'href=[\'"]?([^\'" &gt;]+)', content)<br>
  CrawlDbDatum<br>
  link_datums = []<br>
  for link in outlinks:<br>
    link_datum = LinkDbDatum(parent=key, link_url=link)<br>
    link_datums.append(link_datum)<br>
  ndb.put_multi(link_datums) <br>
  content_links = re.findall(r'src=[\'"]?([^\'" &gt;]+)', content)<br>
  return content_links<br>
</code></pre>
Create a function taking a single argument. It will be called in phase of extract out links.<br>
If function was returns list of urls, which will use to next crawl job's seed urls.<br>
However, It may not always return a list of URL, you can define a function that matches the context.<br>
<h3>6.Setting the fetcher_policy</h3>
fetcher_policy is a configuration of fetch job,that configuration file named fetcher_policy.yaml.<br>
fetcher_policy.yaml is exsting in lakshmi library's resource folder.<br>
<pre><code>laskhmi/<br>
|-- resource/<br>
   |-- fetcher_policy.yaml<br>
</code></pre>
The following is an example of an fetcher_policy.yaml file:<br>
<pre><code>fetcher_policy:<br>
  agent_name: test<br>
  email_address: test@domain.com<br>
  web_address: http://test.domain.com<br>
  min_response_rate: 0<br>
  max_content_size:<br>
  - content_type: default<br>
    size: 100000<br>
  crawl_end_time: 15000<br>
  crawl_delay: 0<br>
  max_redirects: 20<br>
  accept_language: en-us,en-gb,en;q=0.7,*;q=0.3<br>
  valid_mime_types: text/html,text/plain,image/png,image/gif,image/jpeg<br>
  redirect_mode: follow_all<br>
  request_timeout: 20000<br>
</code></pre>
An fetcher_policy element can have the following elements:<br>
<br><br>fetcher_policy: The fetcher policy root.<br>
<br>agent_name: specify for the agent name should be something specific to your organization or use-case, NOT “lakshmi”.<br>
<br>email_address: specify for the email address should be something specific to your organization or use-case.<br>
<br>web_address: specify for the web address should be something specific to your organization or use-case.<br>
<br>min_response_rate: rate of http response.<br>
<br>max_content_size: maximum size of content. that specify for each mime types.(default is means whole mime types)<br>
<br>crawl_end_time: crawl duration you know exactly when the crawl will end.<br>
<br>crawl_delay: crawl delay time of fetch.<br>
<br>max_redirects: maximum count of redirects.<br>
<br>accept_language: restricts the set of natural languages that are preferred as a response to the request.<br>
<br>valid_mime_types: mime types you want to restrict what content type<br>
<br>redirect_mode:<br>
<br>-properties:<br>
<br>  follow_all: Fetcher will try to follow all redirects<br>
<br>  follow_none: No redirects are followed.<br>
<br>request_timeout: timeout fetch job.<br>
<h3>7.Modifying app.yaml to your application settings.</h3>
(In generally, just modified application value only.)<br>
<pre><code>application: your_application_name<br>
</code></pre>
<h3>8.Deploy the demo application to your host.</h3>
<pre><code>appcfg.py update demo/<br>
</code></pre>
<br>
<br>
<h2>Run the demonstration application</h2>
<h3>Running the Web Crawl Job.</h3>
Navigate your browser to<br>
<pre><code>http://&lt;your_app_id&gt;.appspot.com/start?target=http://sample.com/blog/ <br>
</code></pre>
will start web crawl job.<br>
<br>
Viewing the status of your Web crawl job<br>
FetcherPipeline#start() returns a pipeline id. The page<br>
<pre><code>/_ah/pipeline/status.html?root=&lt;PIPELINE_ID&gt;<br>
</code></pre>
on your application will display the status of this pipeline.<br>
<br>
・Start the job<br>
<img src='http://lakshmi.googlecode.com/files/screenshot2.png' />
<br>・Finished the job<br>
<img src='http://lakshmi.googlecode.com/files/screenshot1.png' />

This is the one cycle of web crawl work flow.<br>
The result of crawl job in the FetchedDatum kind,<br>
You will use FetchedDatum.content_text to data mining.<br>
When you would like to run periodically, Use the scheduled tasks with cron.<br>
<br>
<h3>Clean up your datastore</h3>
Navigate your browser to<br>
<pre><code>http://&lt;your_app_id&gt;.appspot.com/clean_all<br>
</code></pre>
will delete fetched data, failed data, unnecessary data(lower scored data).<br>
this job is important to your jobs more efficiency and for your quota limits.<br>
<br>
<h1>Limitations</h1>
The code is a work in progress, and this is an early release. If you run into problems, please don't hesitate to ask questions on the mailing list or to report bugs.<br>
<br>
The current shuffle implementation is limited to datasets that fit entirely in memory on a single instance.<br>
Also, pipeline process is consume a lot of operation to datastore, so recommended to billing settings your application.<br>
<br>
The current code does not offer an overview page that displays all running jobs, but you can find the pipeline job IDs in your application's logs and in the task queue, and view the status as described above.