require 'json'
require 'fileutils'

require_relative '../config/paths'

# File name format: level_0_id_1.dat

class SSTable
  include Singleton

  def initialize
    @dir = Paths::DATA_DIR_PATH
    drop
    @file_id = -1
    Dir.glob(File.join(@dir, '*')).each do |path|
      file_id = path.split('_').last.split('.').first.to_i
      @file_id = file_id if file_id > @file_id
    end
    @file_id += 1
    puts "SSTable initialized with file id: #{@file_id}"
    @file_names = []
    @level_depth = 0
    compute_storage_state
  end

  def write(data)
    path = "#{@dir}/level_0_id_#{@file_id}.dat"
    File.open(path, 'w') do |file|
      data.each do |key, value_of_key|
        file.puts({ key: key, value: value_of_key[:value], timestamp: value_of_key[:timestamp] }.to_json)
      end
    end
    @file_id += 1

    merge_if_needed
    compute_storage_state
  end

  def get(key, lvl = 0)
    # start reading from the lovest level
    # get all the files at the lovest level
    # start reading from the latest file id
    # if data not exist, skip to the next id
    # if data not exist in the level skip to the next level
    # if data not exist in any level, return nil
    # if data exist in the level, return
    # puts "level #{lvl} for key #{key}"

    sorted_files_to_read = @file_names[lvl].sort_by do |filename|
      filename.match(/id_(\d+)/)[1].to_i
    end.reverse

    sorted_files_to_read.each do |file_name|
      data = read(File.join(@dir, file_name))
      return data[key] if data[key]
    end

    if lvl < @level_depth
      puts "level #{lvl} not found, moving to next level"
      get(lvl + 1)
    else
      puts "level #{lvl} not found, no more levels to search"
      nil
    end
  end

  def compute_storage_state
    @file_names = []
    file_names = Dir.glob(File.join(@dir, '*')).map { |path| path.split('/').last }
    file_names.each do |file_name|
      level = file_name.split('_')[1]
      @level_depth = level if level.to_i > @level_depth
      @file_names[level.to_i] = [] unless @file_names[level.to_i]
      @file_names[level.to_i] << file_name
    end
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

  def merge_if_needed
    # TODO: Implement merge
  end
end
