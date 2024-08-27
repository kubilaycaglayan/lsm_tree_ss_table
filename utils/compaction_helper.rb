module CompactionHelper
  def sort_shard_names(all_shard_names)
    all_shard_names.map! do |shard_names|
      if shard_names.nil?
        []
      else
        shard_names.sort_by do |filename|
          filename.match(/id_(\d+)/)[1].to_i
        end.reverse
      end
    end

    all_shard_names
  end

  def last_shard_id(level, all_shard_names)
    return -1 if all_shard_names[level].nil? || all_shard_names[level].empty?

    all_shard_names[level].first.split('_').last.split('.').first.to_i
  end

  def compress_if_needed(all_shard_names, dir, after_merge_callback, target_level = 0)
    should_compress_next_level = false

    while all_shard_names[target_level].size >= Constants::PARTITION_LIMIT_PER_LEVEL do
      shard_recent, shard = all_shard_names[target_level].last(2)
      data_recent = read(File.join(dir, shard_recent))
      data = read(File.join(dir, shard))
      merged_data = merge_sort(data, data_recent)

      path = "#{dir}/level_#{target_level + 1}_id_#{last_shard_id(target_level + 1, all_shard_names) + 1}"
      File.open(path, 'w') do |file|
        merged_data.each do |pair|
          file.puts(pair.to_json)
        end
      end

      FileUtils.rm_rf(File.join(dir, shard_recent))
      FileUtils.rm_rf(File.join(dir, shard))

      all_shard_names = after_merge_callback.call

      should_compress_next_level = true
    end

    compress_if_needed(all_shard_names, dir, after_merge_callback, target_level + 1) if should_compress_next_level
  end

  def read(file_path)
    return @files_memo[file_path] if @files_memo[file_path]

    data = read_from_file(file_path)

    @files_memo[file_path] = data

    data
  end
end
