require 'httparty'
require 'concurrent'
require 'logger'
require 'crawlbase'

class CrawlerPusher
  def initialize(urls_file)
    @urls_file = urls_file
    @logger = Logger.new('log/crawler_pusher.log')
  end

  def push_urls
    urls = File.readlines(@urls_file).map(&:chomp)
    pool = Concurrent::FixedThreadPool.new(10)
    
    urls.each do |url|
      pool.post do
        push_url(url)
      end
    end

    pool.shutdown
    pool.wait_for_termination
  end

  private

  def push_url(url)
    api = Crawlbase::API.new(token: 'f2EP6P45BBXA2C_mXeHV4A')
    response = api.get(url, autoparse: true)

    if response.status_code == 200
      parsed_data = response.body
      @logger.info("Successfully pushed URL: #{url}, Parsed Data: #{parsed_data}")
      data = save_parsed_data(url, parsed_data)
      uri = URI('https://api.crawlbase.com')
      uri.query = URI.encode_www_form({
        token: 'f2EP6P45BBXA2C_mXeHV4A',
        url: 'https://postman-echo.com/post',
        post_content_type: 'application/json;charset=UTF-8'})
      json_data = data
      res = Net::HTTP.post(uri, json_data)
    else
      @logger.error("Failed to push URL: #{url}, Error: #{response.body}")
    end
  rescue => e
    @logger.error("Exception while pushing URL: #{url}, Error: #{e.message}")
  end

  def save_parsed_data(url, data)
    File.open('data/parsed_pages.json', 'a') do |file|
      file.puts({ url: url, data: data }.to_json)
    end
  rescue => e
    @logger.error("Exception while saving parsed data for URL: #{url}, Error: #{e.message}")
  end
end

CrawlerPusher.new('data/asin_urls.txt').push_urls
