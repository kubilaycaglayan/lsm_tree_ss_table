require 'json'
require 'fileutils'

require_relative '../config/paths'
require_relative '../DTOs/pair'
require_relative '../utils/file_helper'
require_relative '../utils/compaction_helper'
require_relative '../utils/sort_helper'
require_relative '../utils/search_helper'

class SSTable
  include Singleton
  include PairDTO
  include FileHelper
  include CompactionHelper
  include SortHelper
  include SearchHelper

  def initialize
    @dir = Paths::DATA_DIR_PATH
    # drop
    @base_shard_id = -1
    compute_shard_id
    @shard_names = []
    @files_memo = {}
    @file_lines_memo = {}
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
    path = "#{@dir}/level_0_id_#{@base_shard_id}"
    File.open(path, 'w') do |file|
      data.each do |key, value_of_key|
        file.puts({ key: key, value: value_of_key[:value], timestamp: value_of_key[:timestamp] }.to_json)
      end
    end
    @base_shard_id += 1

    compute_storage_state
    compress_if_needed(@shard_names, @dir, -> { compute_storage_state })
  end

  def get(key, lvl = 0)
    return nil if @shard_names.empty?

    get(key, lvl + 1) if (@shard_names[lvl].nil? || @shard_names[lvl].empty?) && lvl < @level_depth

    @shard_names[lvl].each do |shard_name|
      result = binary_search_in_file(@file_lines_memo, File.join(@dir, shard_name), key)
      return result unless result.nil? || result[:deleted]
    end

    get(key, lvl + 1) if lvl < @level_depth
  end

  def compute_storage_state
    @shard_names = []
    new_shard_names = Dir.glob(File.join(@dir, '*')).map { |path| path.split('/').last }
    parse_directories_into_shard_names(new_shard_names)
    @files_memo = {}
    @file_lines_memo = {}
    sort_shard_names(@shard_names)
  end

  def parse_directories_into_shard_names(new_shard_names)
    new_shard_names.each do |shard_name|
      level = shard_name.split('_')[1]
      @level_depth = level.to_i if level.to_i > @level_depth
      @shard_names[level.to_i] = [] unless @shard_names[level.to_i]
      @shard_names[level.to_i] << shard_name
    end
  end

  def drop
    Dir.glob(File.join(@dir, '*')).each do |path|
      FileUtils.rm_rf(path) if File.exist?(path)
    end

    compute_storage_state
  end
end
