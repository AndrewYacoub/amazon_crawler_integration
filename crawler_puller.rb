require 'httparty'
require 'logger'
require 'crawler' # Ensure the crawler gem is required

class CrawlerPuller
  CRAWLBASE_STORAGE_URL = 'https://api.crawlbase.com/storage?token=f2EP6P45BBXA2C_mXeHV4A&rid=RID'

  def initialize
    @logger = Logger.new('log/crawler_puller.log')
  end

  def pull_data
    loop do
      fetch_parsed_pages
      sleep 60 # Wait for 60 seconds before fetching again
    end
  end

  private

  def fetch_parsed_pages
    response = HTTParty.get(CRAWLBASE_STORAGE_URL, query: { token: "f2EP6P45BBXA2C_mXeHV4A" })

    if response.success?
      data = response.parsed_response
      File.open('data/parsed_pages.json', 'a') do |file|
        file.puts(data.to_json)
      end
      @logger.info("Fetched parsed pages successfully.")
    else
      @logger.error("Failed to fetch parsed pages, Error: #{response.body}")
    end
  rescue => e
    @logger.error("Exception while fetching parsed pages, Error: #{e.message}")
  end
end

CrawlerPuller.new.pull_data
