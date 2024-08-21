require 'json'

require_relative '../config/paths'

class SSTable
  include Singleton
  def initialize
    @dir = Paths::DATA_DIR_PATH
  end

  def write(data)
    timestamp = Time.now.strftime('%Y%m%d%H%M%S')
    path = "#{@dir}/sstable_#{timestamp}.dat"
    File.open(path, 'w') do |file|
      data.each do |key, value|
        file.puts({ key: key, value: value }.to_json)
      end
    end
  end

  def read
    data = {}
    File.readlines(@file_path).each do |line|
      entry = JSON.parse(line)
      data[entry['key']] = entry['value']
    end
    data
  end

  def drop
    Dir.glob(File.join(@dir, '*')).each do |path|
      FileUtils.rm_rf(path) if File.exist?(path)
    end
  end
end
