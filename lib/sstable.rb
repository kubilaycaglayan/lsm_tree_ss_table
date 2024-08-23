require 'json'
require 'fileutils'

require_relative '../config/paths'
require_relative '../DTOs/pair'

class SSTable
  include Singleton, PairDTO

  def initialize
    @dir = Paths::DATA_DIR_PATH
    drop
    @base_shard_id = -1
    compute_shard_id
    puts "SSTable initialized with shard id: #{@base_shard_id}"
    @shard_names = []
    @level_depth = 0
    compute_storage_state
  end

  def compute_shard_id
    Dir.glob(File.join(@dir, '*')).each do |path|
      shard_id = path.split('_').last.split('.').first.to_i
      @base_shard_id = shard_id if shard_id > @base_shard_id
    end
    @base_shard_id += 1
  end

  def write(data)
    path = "#{@dir}/level_0_id_#{@base_shard_id}.dat"
    File.open(path, 'w') do |file|
      data.each do |key, value_of_key|
        file.puts({ key: key, value: value_of_key[:value], timestamp: value_of_key[:timestamp] }.to_json)
      end
    end
    @base_shard_id += 1

    compute_storage_state
    compact_if_needed
  end

  def get(key, lvl = 0)
    # TODO: use binary search
    # TODO: start from the recent data

    @shard_names[lvl].each do |shard_name|
      data = read(File.join(@dir, shard_name))
      return data[key] if data[key]
    end

    if lvl < @level_depth
      puts "level #{lvl} not found, moving to next level #{key}"
      get(key, lvl + 1)
    else
      puts "level #{lvl} not found, no more levels to search #{key}"
      nil
    end
  end

  def compute_storage_state
    @shard_names = []
    shard_names = Dir.glob(File.join(@dir, '*')).map { |path| path.split('/').last }
    shard_names.each do |shard_name|
      level = shard_name.split('_')[1]
      @level_depth = level.to_i if level.to_i > @level_depth
      @shard_names[level.to_i] = [] unless @shard_names[level.to_i]
      @shard_names[level.to_i] << shard_name
    end
    sort_shard_names
  end

  def sort_shard_names
    @shard_names.map! do |shard_names|
      if shard_names.nil?
        []
      else
        shard_names.sort_by do |filename|
          filename.match(/id_(\d+)/)[1].to_i
        end.reverse
      end
    end
  end

  def last_shard_id(level)
    return -1 if @shard_names[level].nil? || @shard_names[level].empty?

    @shard_names[level].first.split('_').last.split('.').first.to_i
  end

  def read(file_path)
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

  def drop
    Dir.glob(File.join(@dir, '*')).each do |path|
      FileUtils.rm_rf(path) if File.exist?(path)
    end
  end

  def compact_if_needed(target_level = 0)
    should_compact_next_level = false

    while @shard_names[target_level].size >= Constants::SHARD_MULTIPLIER_FACTOR_PER_LEVEL do
      puts "Compacting level #{target_level}"
      shard_recent, shard = @shard_names[target_level].last(2)
      data_recent = read(File.join(@dir, shard_recent))
      data = read(File.join(@dir, shard))
      merged_data = merge_sort(data, data_recent)

      path = "#{@dir}/level_#{target_level + 1}_id_#{last_shard_id(target_level + 1) + 1}"
      File.open(path, 'w') do |file|
        merged_data.each do |pair|
          file.puts(pair.to_json)
        end
      end

      puts "Compacting level #{target_level} finished - #{path}"

      FileUtils.rm_rf(File.join(@dir, shard_recent))
      FileUtils.rm_rf(File.join(@dir, shard))

      compute_storage_state

      should_compact_next_level = true
    end

    compact_if_needed(target_level + 1) if should_compact_next_level
  end

  def merge_sort(data, data_recent)
    puts "Merge sort started, #{data.size} - #{data_recent.size}"

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
          puts "Skipping deleted key #{key}"
          next
        elsif data_recent[key_recent][:deleted]
          j += 1
          puts "Skipping deleted key #{key_recent}"
          next
        elsif (key <=> key_recent).zero?
          result << key_to_hash(key, data)
          i += 1
          j += 1
          puts "keys equal #{key} - #{key_recent}"
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

    puts "Merge sort finished, #{result.size}"
    # print result
    result
  end
end
