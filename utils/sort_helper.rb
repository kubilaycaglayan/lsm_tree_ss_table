module SortHelper
  def merge_sort(data, data_recent)
    result = []
    i, j = 0, 0

    keys = data.keys
    keys_recent = data_recent.keys

    while i < keys.size || j < keys_recent.size
      key = keys[i]
      key_recent = keys_recent[j]

      if key && key_recent
        if data[key][:deleted]
          i += 1
          next
        elsif data_recent[key_recent][:deleted]
          j += 1
          next
        elsif (key <=> key_recent).zero?
          result << key_to_hash(key, data)
          i += 1
          j += 1
        elsif (key <=> key_recent) == -1
          result << key_to_hash(key, data)
          i += 1
        else
          result << key_to_hash(key_recent, data_recent)
          j += 1
        end
      elsif key
        result << key_to_hash(key, data)
        i += 1
      elsif key_recent
        result << key_to_hash(key_recent, data_recent)
        j += 1
      end
    end

    result
  end
end
