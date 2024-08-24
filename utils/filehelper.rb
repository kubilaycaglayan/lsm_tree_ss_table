module FileHelper
  def read_from_file(file_path)
    data = {}
    File.readlines(file_path).each do |line|
      entry = JSON.parse(line)
      data[entry['key']] = {
        value: entry['value'],
        timestamp: entry['timestamp']
      }

      data[entry['key']][:deleted] = true if entry['deleted']
    end

    data
  end
end
