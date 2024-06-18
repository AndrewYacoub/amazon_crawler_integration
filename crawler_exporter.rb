require 'json'
require 'csv'
require 'logger'

class CrawlerExporter
  def initialize(json_file, csv_file)
    @json_file = json_file
    @csv_file = csv_file
    @logger = Logger.new('log/crawler_exporter.log')
  end

  def export_to_csv
    data = []
    File.foreach(@json_file) do |line|
      data << JSON.parse(line)
    end

    CSV.open(@csv_file, 'w') do |csv|
      csv << data.first.keys # Add headers
      data.each { |hash| csv << hash.values }
    end

    @logger.info("Exported data to CSV successfully.")
  rescue => e
    @logger.error("Exception while exporting data to CSV, Error: #{e.message}")
  end
end

CrawlerExporter.new('data/parsed_pages.json', 'data/exported_data.csv').export_to_csv
