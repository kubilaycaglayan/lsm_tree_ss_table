module PairDTO
  def key_to_hash(key, data)
    { key: key, value: data[key][:value], timestamp: data[key][:timestamp] }
  end
end
