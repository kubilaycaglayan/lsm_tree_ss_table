module SearchHelper
  def binary_search_in_file(file_lines_memo, file_path, key)
    if file_lines_memo[file_path]
      lines = file_lines_memo[file_path]
    else
      lines = File.readlines(file_path)
      file_lines_memo[file_path] = lines
    end

    left, right = 0, lines.size - 1

    while left <= right
      mid = (left + right) / 2
      entry = JSON.parse(lines[mid])

      if entry['key'] == key
        return {
          value: entry['value'],
          timestamp: entry['timestamp'],
          deleted: entry['deleted'] || false
        }
      elsif entry['key'] > key
        right = mid - 1
      else
        left = mid + 1
      end
    end

    nil
  end
end
