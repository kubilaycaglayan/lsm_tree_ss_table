require 'json'

class SSTable
  def initialize(file_path)
    @file_path = file_path
  end

  def write(data)
    File.open(@file_path, 'w') do |file|
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
end
